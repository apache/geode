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
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * TRAC #36526: getActiveServers can return wrong answer
 */
@Category({ClientServerTest.class})
public class GetConnectedServerCountRegressionTest implements Serializable {

  private String uniqueName;
  private String hostName;

  private int port1;
  private int port2;
  private int port3;

  private VM server1;
  private VM server2;
  private VM server3;

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
    server3 = getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();

    port1 = server1.invoke(() -> createServerCache());
    port2 = server2.invoke(() -> createServerCache());
    port3 = server3.invoke(() -> createServerCache());

    createClientCache();
    awaitConnectedServerCount(3);
  }

  @Test
  public void getConnectedServerCountShouldNotIncludeStoppedServer() {
    server2.invoke(() -> stopServer());
    awaitConnectedServerCount(2);
  }

  private int createServerCache() throws Exception {
    cacheRule.createCache();

    // no region is created on server

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCache() {
    clientCacheRule.createClientCache();

    PoolImpl pool = (PoolImpl) PoolManager.createFactory().addServer(hostName, port1)
        .addServer(hostName, port2).addServer(hostName, port3).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1).setSubscriptionMessageTrackingTimeout(54321)
        .setIdleTimeout(-1).setPingInterval(200).create(uniqueName);

    ClientRegionFactory clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    clientRegionFactory.setPoolName(pool.getName());
  }

  private void stopServer() {
    cacheRule.getCache().getCacheServers().iterator().next().stop();
  }

  private void awaitConnectedServerCount(final int expectedServerCount) {
    await()
        .until(() -> findPool(uniqueName).getConnectedServerCount() == expectedServerCount);
  }

  private PoolImpl findPool(String name) {
    return (PoolImpl) PoolManager.find(name);
  }
}
