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

import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * When server is running but region is not created on server. Client sends register interest
 * request, server checks for region, and if region is not exist on server, it throws an exception
 * to the client. Hence, client marks server as dead.
 *
 * <p>
 * To avoid this, there should not be any check of region before registration. And region
 * registration should not fail due to non existent region.
 *
 * <p>
 * TRAC #36805: Interest registration on server should not check for region in server's cache.
 */
@Category({ClientServerTest.class})
public class RegisterInterestOnServerWithoutRegionRegressionTest implements Serializable {

  private String uniqueName;
  private String regionName;
  private String hostName;

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
    client = getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    hostName = getHostName();

    int port = server.invoke(() -> createServerCache());

    client.invoke(() -> createClientCache(port));

    client.invoke(() -> awaitConnectedServerCount(1));
  }

  @Test
  public void registerInterestDoesNotRequireRegionOnServer() {
    // register interest should not cause any failure
    assertThatCode(() -> client.invoke(() -> registerInterest())).doesNotThrowAnyException();
  }

  private int createServerCache() throws Exception {
    cacheRule.createCache();

    // no region is created on server

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCache(int port) {
    clientCacheRule.createClientCache();

    PoolImpl pool = (PoolImpl) PoolManager.createFactory().addServer(hostName, port)
        .setSubscriptionEnabled(true).setMinConnections(4).create(uniqueName);

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    crf.setPoolName(pool.getName());

    crf.create(regionName);
  }

  private void registerInterest() {
    try (IgnoredException ie = addIgnoredException(RegionDestroyedException.class)) {
      Region<Object, ?> region = clientCacheRule.getClientCache().getRegion(regionName);
      assertNotNull(region);

      List<String> listOfKeys = new ArrayList<>();
      listOfKeys.add("key-1");
      listOfKeys.add("key-2");
      listOfKeys.add("key-3");
      listOfKeys.add("key-4");
      listOfKeys.add("key-5");

      assertThatThrownBy(() -> region.registerInterest(listOfKeys))
          .isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(RegionDestroyedException.class);
    }
  }

  private void awaitConnectedServerCount(final int expectedServerCount) {
    PoolImpl pool = (PoolImpl) PoolManager.getAll().get(uniqueName);
    assertThat(pool).isNotNull();

    await().until(() -> pool.getConnectedServerCount() == expectedServerCount);
  }
}
