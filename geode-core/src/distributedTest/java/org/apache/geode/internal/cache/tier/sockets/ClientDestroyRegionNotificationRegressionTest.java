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
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * The Region Destroy Operation from Cache Client does not pass the Client side Context object nor
 * does the p2p messaging has provision of sending Context object in the DestroyRegionMessage. This
 * can cause sender to receive it own region destruction message.
 *
 * <p>
 * TRAC #36269: RegionDestroy operation may get sent to the originator
 */
@Category(ClientServerTest.class)
public class ClientDestroyRegionNotificationRegressionTest implements Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;

  private int port1;
  private int port2;

  private VM server1;
  private VM server2;

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
    server1 = getVM(0);
    server2 = getVM(1);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";

    port1 = server1.invoke(() -> createServerCache());
    port2 = server2.invoke(() -> createServerCache());

    createClientCacheAndDestroyRegion();
  }

  /**
   * This tests whether the region destroy are not received by the sender
   */
  @Test
  public void senderDoesNotReceiveRegionDestroy() throws Exception {
    server1.invoke(() -> {
      await().until(() -> cacheRule.getCache().getRegion(regionName) == null);
    });
    server2.invoke(() -> {
      await().until(() -> cacheRule.getCache().getRegion(regionName) == null);
    });

    Thread.sleep(5 * 1000);

    assertThat(clientCacheRule.getClientCache().getRegion(regionName).isDestroyed()).isFalse();
  }

  private void createClientCacheAndDestroyRegion() {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl pool;
    try {
      pool = (PoolImpl) PoolManager.createFactory().addServer(hostName, port1)
          .addServer(hostName, port2).setSubscriptionEnabled(true).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(4).create(uniqueName);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    ClientRegionFactory<?, ?> clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    clientRegionFactory.setPoolName(pool.getName());
    clientRegionFactory.create(regionName);

    Connection connection = pool.acquireConnection(new ServerLocation(hostName, port2));
    EventID eventId = new EventID(new byte[] {1}, 1, 1);
    ServerRegionProxy serverRegionProxy = new ServerRegionProxy(regionName, pool);

    serverRegionProxy.destroyRegionOnForTestsOnly(connection, eventId, null);
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
    regionFactory.create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
