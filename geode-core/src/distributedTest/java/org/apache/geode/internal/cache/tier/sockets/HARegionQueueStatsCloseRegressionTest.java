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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Verifies the fix for bug 37210. Reason for the bug was that HARegionQueue's destroy was not
 * being called on CacheClientProxy's closure. As a result, stats were left open.
 *
 * <p>
 * TRAC #37210: HARegionQueueStats are never closed
 */
@Category(ClientServerTest.class)
public class HARegionQueueStatsCloseRegressionTest implements Serializable {

  private String uniqueName;
  private String regionName;
  private String hostName;

  private int port;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    hostName = getHostName();

    port = server.invoke(() -> createServerCache());
  }

  /**
   * This test does the following: <br>
   * 1) Create the client <br>
   * 2) Do some operations from the cache-server <br>
   * 3) Stop the primary cache-server <br>
   * 4) Explicitly close the CacheClientProxy on the server. <br>
   * 5) Verify that HARegionQueue stats are closed and entry for the ha region is removed from
   * dispatchedMessagesMap.
   */
  @Test
  public void haRegionQueueClosesStatsWhenClientProxyIsClosed() {
    addIgnoredException(SocketException.class);
    addIgnoredException(IOException.class);

    client.invoke(() -> createClientCache());
    server.invoke(() -> doEntryOperations());

    server.invoke(() -> closeProxyAndVerifyHARegionQueueStatsAreClosed());
    client.invoke(() -> clientCacheRule.getClientCache().close());

    server.invoke(() -> verifyDispatchedMessagesMapIsEmpty());
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
    regionFactory.create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setMaximumTimeBetweenPings(1000000);
    cacheServer.setNotifyBySubscription(false);
    cacheServer.setPort(0);
    cacheServer.setSocketBufferSize(32768);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCache() {
    clientCacheRule.createClientCache();

    Pool pool = PoolManager.createFactory().addServer(hostName, port).setSubscriptionEnabled(true)
        .setThreadLocalConnections(true).setReadTimeout(10000).setSocketBufferSize(32768)
        .setMinConnections(3).setSubscriptionRedundancy(-1).create(uniqueName);

    ClientRegionFactory<Object, ?> clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    clientRegionFactory.setPoolName(pool.getName());

    Region<Object, ?> region = clientRegionFactory.create(regionName);
    region.registerInterest("ALL_KEYS");
  }

  /**
   * Close the CacheClientProxy of the client on the server and verify that ha-stats are closed and
   * the entry for the region is removed from dispatchedMessagesMap.
   */
  private void closeProxyAndVerifyHARegionQueueStatsAreClosed() {
    assertThat(cacheRule.getCache().getCacheServers()).hasSize(1);

    InternalCacheServer cacheServer =
        (InternalCacheServer) cacheRule.getCache().getCacheServers().iterator().next();

    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    Iterator proxies =
        cacheServer.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();

    assertThat(proxies.hasNext()).isTrue();

    CacheClientProxy proxy = (CacheClientProxy) proxies.next();
    Map dispatchedMsgMap = HARegionQueue.getDispatchedMessagesMapForTesting();
    HARegionQueue haRegionQueue = proxy.getHARegionQueue();

    assertThat(dispatchedMsgMap.get(haRegionQueue.getRegion().getName())).isNotNull();
    assertThat(haRegionQueue.getStatistics().isClosed()).isFalse();

    proxy.close();

    assertThat(haRegionQueue.getStatistics().isClosed()).isTrue();
  }

  private void verifyDispatchedMessagesMapIsEmpty() {
    await()
        .untilAsserted(
            () -> assertThat(HARegionQueue.getDispatchedMessagesMapForTesting()).isEmpty());
  }

  private void doEntryOperations() {
    Region<String, String> region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < 10; i++) {
      region.put("server-" + i, "server-" + "val-" + i);
    }
  }
}
