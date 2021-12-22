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
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(ClientServerTest.class)
public class ClientRegionGetRegressionTest implements Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;

  private int port;
  private VM server;
  private static DUnitBlackboard blackboard;
  private final String key = "KEY-1";

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";

    port = server.invoke(this::createServerCache);
    createClientCache();
  }

  @After
  public void tearDown() {
    blackboard.initBlackboard();
  }

  @Test
  public void getOnProxyRegionFromMultipleThreadsReturnsDifferentObjects() throws Exception {

    ClientRegionFactory<String, String> crf = clientCacheRule.getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.create(regionName);

    Future get1 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isNotSameAs(get2value);
  }

  @Test
  public void getOnCachingProxyRegionFromMultipleThreadsReturnSameObject() throws Exception {

    ClientRegionFactory<String, String> crf = clientCacheRule.getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.create(regionName);
    Future get1 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isSameAs(get2value);
  }

  @Test
  public void getOnCachingProxyRegionWithCopyOnReadFromMultipleThreadsReturnsDifferentObject()
      throws Exception {

    ClientCache cache = clientCacheRule.getClientCache();
    cache.setCopyOnRead(true);

    ClientRegionFactory<String, String> crf =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.create(regionName);
    Future get1 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isNotSameAs(get2value);
  }

  private void createClientCache() {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory().addPoolServer(hostName, port);
    clientCacheRule.createClientCache(clientCacheFactory);
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
    regionFactory.setCacheLoader(new TestCacheLoader()).create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  public class TestObject implements Serializable {
    int id;

    TestObject(int id) {
      this.id = id;
    }
  }

  private class TestCacheLoader implements CacheLoader, Serializable {
    @Override
    public synchronized Object load(LoaderHelper helper) {
      getBlackboard().signalGate("Loader");
      return new TestObject(1);
    }

    @Override
    public void close() {}
  }
}
