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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class ClientServerNotColocatedTransactionDistributedTest implements Serializable {
  private String hostName;
  private String uniqueName;
  private String regionName;
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
  public void setUp() {
    server1 = getVM(0);
    server2 = getVM(1);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
  }

  @Test
  public void getEntryOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    int initialPuts = 4;
    setupClientAndServerForMultipleTransactions(initialPuts);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      Throwable caughtException = catchThrowable(() -> doTransactionalGets(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void setupClientAndServerForMultipleTransactions(int initialPuts) {
    int port1 = server1.invoke(() -> createServerRegion(2, false, 0));
    int port2 = server2.invoke(() -> createServerRegion(2, false, 0));
    server1.invoke(() -> doPuts(initialPuts));

    createClientRegion(port1, port2);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor, int redundancy)
      throws Exception {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets).setRedundantCopies(redundancy);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int... ports) {
    clientCacheRule.createClientCache();
    PoolImpl pool = getPool(ports);
    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);
  }

  private void doPuts(int numOfEntries) {
    for (int i = 0; i <= numOfEntries; i++) {
      cacheRule.getCache().getRegion(regionName).put(i, i);
    }
  }

  private PoolImpl getPool(int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setReadTimeout(12000).setSocketBufferSize(1000);

    return (PoolImpl) factory.create(uniqueName);
  }

  private void doTransactionalGets(int initialPuts, Region<Integer, Integer> region) {
    for (int i = 1; i < initialPuts; i++) {
      region.get(i);
    }
  }

  @Test
  public void getAllOnMultipleNodesInATransactionThrowsTransactionDataNotColocatedException() {
    int initialPuts = 4;
    setupClientAndServerForMultipleTransactions(initialPuts);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      Throwable caughtException = catchThrowable(() -> doTransactionalGetAll(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalGetAll(int initialPuts, Region<Integer, Integer> region) {
    List<Integer> list = new ArrayList<>();
    for (int i = 1; i < initialPuts; i++) {
      list.add(i);
    }
    region.getAll(list);
  }

  @Test
  public void putOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    int initialPuts = 4;
    setupClientAndServerForMultipleTransactions(initialPuts);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      Throwable caughtException = catchThrowable(() -> doTransactionalPuts(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalPuts(int initialPuts, Region<Integer, Integer> region) {
    for (int i = 1; i < initialPuts; i++) {
      region.put(i, i + 100);
    }
  }

  @Test
  public void putAllOnMultipleNodesInATransactionThrowsTransactionDataNotColocatedException() {
    int initialPuts = 4;
    setupClientAndServerForMultipleTransactions(initialPuts);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      Throwable caughtException = catchThrowable(() -> doTransactionalPutAll(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalPutAll(int initialPuts, Region<Integer, Integer> region) {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 1; i < initialPuts; i++) {
      map.put(i, i + 100);
    }
    region.putAll(map);
  }
}
