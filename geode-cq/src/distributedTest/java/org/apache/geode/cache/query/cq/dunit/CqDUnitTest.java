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
package org.apache.geode.cache.query.cq.dunit;

import static junit.framework.TestCase.assertEquals;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class CqDUnitTest implements Serializable {
  private static final int VM_COUNT = 8;

  private final String REGION_NAME = "region";

  private String hostName;
  private VM[] clients;
  private int serverPort;
  private TestCqListener[] testListener;
  private int totalPut;
  private int totalCQInvocations;
  private final String cqName = "cqName";

  @Rule
  public DistributedRule distributedRule = new DistributedRule(VM_COUNT);

  @Rule
  public CacheRule cacheRule = new CacheRule(VM_COUNT);

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule(VM_COUNT);

  @Rule
  public SerializableTemporaryFolder tempDir = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    hostName = getHostName();
    VM server = getVM(0);
    getVM(0).invoke(() -> cacheRule.createCache());
    testListener = new TestCqListener[VM_COUNT - 1];

    clients = new VM[VM_COUNT - 1];
    for (int i = 0; i < VM_COUNT - 1; i++) {
      clients[i] = getVM(i + 1);
      clients[i].invoke(() -> clientCacheRule.createClientCache());
      testListener[i] = new TestCqListener();
    }

    serverPort = server.invoke(() -> createSubscriptionServer(cacheRule.getCache()));

    for (VM client : clients) {
      client.invoke(this::createRegionOnClient);
    }

    totalPut = 500;
    totalCQInvocations = totalPut * clients.length;
  }

  @Test
  public void clientCanInvokeCQListenersWhenHAContainerEnablesEviction() throws Exception {
    registerCqs();

    AsyncInvocation<?>[] asyncInvocations = new AsyncInvocation[VM_COUNT - 1];
    for (int i = 0; i < VM_COUNT - 1; i++) {
      asyncInvocations[i] = clients[i].invokeAsync(this::doPuts);
    }

    for (int i = 0; i < VM_COUNT - 1; i++) {
      asyncInvocations[i].await();
    }

    for (int i = 0; i < VM_COUNT - 1; i++) {
      clients[i].invoke(this::verifyCQListenerInvocations);
    }
  }

  private void verifyCQListenerInvocations() {
    await().untilAsserted(() -> {
      QueryService cqService = clientCacheRule.getClientCache().getQueryService();
      CqListener cqListener = cqService.getCq(cqName).getCqAttributes().getCqListener();

      assertEquals(totalCQInvocations, ((TestCqListener) cqListener).numEvents.get());
    });
  }

  private void registerCqs() {
    for (int i = 0; i < VM_COUNT - 1; i++) {
      registerCq(clients[i]);
    }
  }

  private void registerCq(VM client) {
    client.invoke(() -> {
      ClientCache clientCache = clientCacheRule.getClientCache();

      QueryService queryService = clientCache.getQueryService();
      CqAttributesFactory cqaf = new CqAttributesFactory();

      int i = client.getId() - 1;
      cqaf.addCqListener(testListener[i]);
      CqAttributes cqAttributes = cqaf.create();

      queryService.newCq(cqName, "Select * from " + SEPARATOR + REGION_NAME + " where ID > 0",
          cqAttributes)
          .executeWithInitialResults();
    });
  }

  private void doPuts() {
    ClientCache clientCache = clientCacheRule.getClientCache();
    Region<Object, Object> region = clientCache.getRegion(REGION_NAME);


    for (int i = totalPut; i > 0; i--) {
      doPut(region, i);
    }
  }

  private void doPut(Region<Object, Object> region, int i) {
    region.put(i, new Portfolio(i));
  }

  private static class TestCqListener implements CqListener, Serializable {
    AtomicInteger numEvents = new AtomicInteger();

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents.incrementAndGet();
    }

    @Override
    public void onError(CqEvent aCqEvent) {}
  }

  private void createRegionOnClient() {
    Pool pool = PoolManager.createFactory().addServer(hostName, serverPort)
        .setSubscriptionEnabled(true).create("poolName");

    ClientRegionFactory<Object, Object> crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.setPoolName(pool.getName());
    crf.create(REGION_NAME);
  }

  private int createSubscriptionServer(InternalCache cache) throws IOException {
    initializeDiskStore(cache);
    createRegionOnServer(cache);
    return initializeCacheServerWithSubscription(cache);
  }

  private void initializeDiskStore(InternalCache cache) throws IOException {
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    diskStoreAttributes.name = "clientQueueDS";
    diskStoreAttributes.diskDirs = new File[] {tempDir.newFolder(testName + "_dir")};
    cache.createDiskStoreFactory(diskStoreAttributes).create("clientQueueDS");
  }

  private void createRegionOnServer(InternalCache cache) {
    cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
  }

  private int initializeCacheServerWithSubscription(InternalCache cache) throws IOException {
    CacheServer cacheServer = cache.addCacheServer();
    ClientSubscriptionConfig clientSubscriptionConfig = cacheServer.getClientSubscriptionConfig();
    clientSubscriptionConfig.setEvictionPolicy("entry");
    clientSubscriptionConfig.setCapacity(1);
    clientSubscriptionConfig.setDiskStoreName("clientQueueDS");
    cacheServer.setPort(0);
    cacheServer.setHostnameForClients(hostName);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
