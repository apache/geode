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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
@SuppressWarnings("serial")
public class ClientWithInterestFailoverDistributedTest implements Serializable {

  private static final String PROXY_REGION_NAME = "PROXY_REGION_NAME";
  private static final String CACHING_PROXY_REGION_NAME = "CACHING_PROXY_REGION_NAME";
  private static final String REGEX = ".*";

  private static InternalCache cache;
  private static InternalClientCache clientCache;

  private int serverPort1;
  private int serverPort2;
  private int primaryServerPort;

  private VM client;
  private VM server;
  private VM server2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setUp() throws Exception {
    client = getVM(0);

    server = getVM(1);
    server2 = getVM(2);

    primaryServerPort = givenTwoCacheServers();
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    cache = null;
    invokeInEveryVM(() -> cache = null);

    clientCache = null;
    invokeInEveryVM(() -> clientCache = null);
  }

  @Test
  public void clientWithSingleKeyInterestFailsOver() {
    client.invoke(() -> registerKey(PROXY_REGION_NAME, 1));
    client.invoke(() -> registerKey(CACHING_PROXY_REGION_NAME, 1));

    performFailoverTesting();
  }

  @Test
  public void clientWithKeyListInterestFailsOver() {
    client.invoke(() -> registerKeys(PROXY_REGION_NAME, 1, 2));
    client.invoke(() -> registerKeys(CACHING_PROXY_REGION_NAME, 1, 2));

    performFailoverTesting();
  }

  @Test
  public void clientWithRegexInterestFailsOver() {
    client.invoke(() -> registerRegex(PROXY_REGION_NAME));
    client.invoke(() -> registerRegex(CACHING_PROXY_REGION_NAME));

    performFailoverTesting();
  }

  private void performFailoverTesting() {
    // arrange
    VM primaryServerVM = getPrimaryServerVM();
    VM secondaryServerVM = getSecondaryServerVM();
    primaryServerVM.invoke(() -> awaitServerMetaDataToContainClient());
    primaryServerVM.invoke(() -> validateServerMetaDataKnowsThatClientRegisteredInterest());
    primaryServerVM.invoke(() -> validateServerMetaDataKnowsWhichClientRegionIsEmpty());

    // act
    primaryServerVM.invoke(() -> stopCacheServer());

    // assert
    secondaryServerVM.invoke(() -> awaitServerMetaDataToContainClient());
    secondaryServerVM.invoke(() -> validateServerMetaDataKnowsThatClientRegisteredInterest());
    secondaryServerVM.invoke(() -> validateServerMetaDataKnowsWhichClientRegionIsEmpty());
  }

  private int createServerCache() throws IOException {
    cache = (InternalCache) new CacheFactory().create();

    RegionFactory<Integer, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    regionFactory.create(PROXY_REGION_NAME);
    regionFactory.create(CACHING_PROXY_REGION_NAME);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  /**
   * Create client cache and return the client connection pool's primary server port
   */
  private int createClientCacheWithTwoRegions(final String host1, final int port1,
      final String host2, final int port2) {
    clientCache = (InternalClientCache) new ClientCacheFactory().create();
    assertThat(clientCache.isClient()).isTrue();

    PoolFactory poolFactory = createPoolFactory();
    poolFactory.addServer(host1, port1);
    poolFactory.addServer(host2, port2);

    Pool pool = poolFactory.create(getClass().getSimpleName() + "-Pool");

    createRegionOnClient(PROXY_REGION_NAME, ClientRegionShortcut.PROXY, pool);
    createRegionOnClient(CACHING_PROXY_REGION_NAME, ClientRegionShortcut.CACHING_PROXY, pool);

    return ((PoolImpl) pool).getPrimaryPort();
  }

  private PoolFactory createPoolFactory() {
    return PoolManager.createFactory().setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setReadTimeout(10000)
        .setSocketBufferSize(32768);
  }

  private void createRegionOnClient(final String regionName, final ClientRegionShortcut shortcut,
      final Pool pool) {
    ClientRegionFactory<Integer, Object> regionFactory =
        clientCache.createClientRegionFactory(shortcut);
    regionFactory.setPoolName(pool.getName());
    Region<Integer, Object> region = regionFactory.create(regionName);
    assertThat(region.getAttributes().getCloningEnabled()).isFalse();
  }

  private int givenTwoCacheServers() {
    serverPort1 = server.invoke(() -> createServerCache());
    serverPort2 = server2.invoke(() -> createServerCache());

    return client.invoke(() -> createClientCacheWithTwoRegions(getServerHostName(), serverPort1,
        getServerHostName(), serverPort2));
  }

  private VM getPrimaryServerVM() {
    assertThat(primaryServerPort).isGreaterThan(-1);
    if (primaryServerPort == serverPort1) {
      return server;
    } else {
      return server2;
    }
  }

  private VM getSecondaryServerVM() {
    assertThat(primaryServerPort).isGreaterThan(-1);
    if (primaryServerPort == serverPort1) {
      return server2;
    } else {
      return server;
    }
  }

  private void registerKey(final String regionName, final int key) {
    Region<Integer, Object> region = clientCache.getRegion(regionName);
    region.registerInterest(key);
  }

  private void registerKeys(final String regionName, final int... keys) {
    Region<Object, ?> region = clientCache.getRegion(regionName);

    List<Integer> list = new ArrayList<>();
    for (int key : keys) {
      list.add(key);
    }

    region.registerInterest(list);
  }

  private void registerRegex(final String regionName) {
    Region<Integer, Object> region = clientCache.getRegion(regionName);
    region.registerInterestRegex(REGEX);
  }

  private void stopCacheServer() {
    getCacheServer().stop();
  }

  private void awaitServerMetaDataToContainClient() {
    await()
        .untilAsserted(() -> assertThat(
            getCacheServer().getAcceptor().getCacheClientNotifier().getClientProxies().size())
                .isEqualTo(1));

    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy).isNotNull();

    await().until(() -> getClientProxy().isAlive() && getClientProxy()
        .getRegionsWithEmptyDataPolicy().containsKey(Region.SEPARATOR + PROXY_REGION_NAME));
  }

  private void validateServerMetaDataKnowsThatClientRegisteredInterest() {
    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy.hasRegisteredInterested()).isTrue();
  }

  private void validateServerMetaDataKnowsWhichClientRegionIsEmpty() {
    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy.getRegionsWithEmptyDataPolicy())
        .containsKey(Region.SEPARATOR + PROXY_REGION_NAME);
    assertThat(proxy.getRegionsWithEmptyDataPolicy())
        .doesNotContainKey(Region.SEPARATOR + CACHING_PROXY_REGION_NAME);
    assertThat(proxy.getRegionsWithEmptyDataPolicy()).hasSize(1);
    assertThat(proxy.getRegionsWithEmptyDataPolicy())
        .containsEntry(Region.SEPARATOR + PROXY_REGION_NAME, 0);
  }

  private InternalCacheServer getCacheServer() {
    return (InternalCacheServer) cache.getCacheServers().iterator().next();
  }

  private CacheClientProxy getClientProxy() {
    CacheClientNotifier notifier = getCacheServer().getAcceptor().getCacheClientNotifier();
    return notifier.getClientProxies().stream().findFirst().orElse(null);
  }
}
