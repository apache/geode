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
package org.apache.geode.management;

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class CachePerfStatsTest implements Serializable {

  private static final int NON_EXISTENT_KEY = 1234;

  private String hostName;
  private String uniqueName;
  private String regionNamePartitioned;
  private String regionNameReplicated;
  private VM server1;
  private VM server2;
  private int port1;

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
    regionNamePartitioned = uniqueName + "_partition_region";
    regionNameReplicated = uniqueName + "_replicated_region";
  }

  @Test
  public void testGetsRatesPartitioned() {
    port1 = server1.invoke(this::createServerRegion);
    createClientRegion(port1);

    int numOfOps = 10;
    int numOfMissedOps = 3;
    putData(numOfOps, regionNamePartitioned);

    getData(numOfOps, regionNamePartitioned);
    getDataMissed(numOfMissedOps, regionNamePartitioned);

    server1.invoke(
        () -> validateGetsRates(regionNamePartitioned, numOfOps + numOfMissedOps, numOfMissedOps));
  }

  @Test
  public void testGetsRatesReplicated() {
    port1 = server1.invoke(this::createReplicatedServerRegion);
    createClientReplicatedRegion(port1);

    int numOfOps = 11;
    int numOfMissedOps = 20;
    putData(numOfOps, regionNameReplicated);

    getData(numOfOps, regionNameReplicated);
    getDataMissed(numOfMissedOps, regionNameReplicated);

    server1.invoke(() -> validateGetsRatesReplicate(regionNameReplicated, numOfOps + numOfMissedOps,
        numOfMissedOps));
  }

  @Test
  public void testGetsRatesTransactionAndReplicated() {
    port1 = server1.invoke(this::createReplicatedServerRegion);
    createClientReplicatedRegion(port1);

    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    int numOfOps = 10;
    int numOfMissedOps = 3;

    txManager.begin();
    putData(numOfOps, regionNameReplicated);
    txManager.commit();

    txManager.begin();
    getData(numOfOps, regionNameReplicated);
    getDataMissed(numOfMissedOps, regionNameReplicated);
    txManager.commit();

    server1.invoke(() -> validateGetsRatesReplicate(regionNameReplicated, numOfOps + numOfMissedOps,
        numOfMissedOps));
  }

  @Test
  public void testGetsRatesTransactionAndPartitioned() {
    port1 = server1.invoke(this::createServerRegion);
    createClientRegion(port1);

    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    int numOfOps = 10;
    int numOfMissedOps = 3;

    txManager.begin();
    putData(numOfOps, regionNamePartitioned);
    txManager.commit();

    txManager.begin();
    getData(numOfOps, regionNamePartitioned);
    getDataMissed(numOfMissedOps, regionNamePartitioned);
    txManager.commit();

    server1.invoke(
        () -> validateGetsRates(regionNamePartitioned, numOfOps + numOfMissedOps, numOfMissedOps));
  }

  @Test
  public void testGetsRatesTransactionAndPartitionedMissLocallyAndGetRemotely() {
    port1 = server1.invoke(this::createServerRegion);
    server2.invoke(this::createServerRegion);
    createClientRegion(port1);

    int numOfOps = 10;
    putData(numOfOps, regionNamePartitioned);

    getDataTransaction(numOfOps, regionNamePartitioned);

    long numberOfMisses = server2.invoke(() -> {
      InternalCache cache = cacheRule.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionNamePartitioned);
      PartitionedRegionDataStore dataStore = region.getDataStore();

      long numberOfEntries = 0;
      for (int i = 0; i <= 113; i++) {
        if (dataStore.getLocalBucketById(i) != null) {
          numberOfEntries++;
        }
      }
      return numberOfEntries;
    });

    server1.invoke(
        () -> validateGetsRates(regionNamePartitioned, numOfOps, (int) numberOfMisses));
  }

  private void putData(int numberOfEntries, String name) {
    Region<Object, Object> region = clientCacheRule.getClientCache().getRegion(name);
    for (int key = 0; key < numberOfEntries; key++) {
      String value = "value" + key;
      region.put(key, value);
    }
  }

  private void getData(int numberOfEntries, String name) {
    Region<Object, Object> region = clientCacheRule.getClientCache().getRegion(name);
    for (int key = 0; key < numberOfEntries; key++) {
      region.get(key);
    }
  }

  private void getDataTransaction(int numberOfEntries, String name) {
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Object, Object> region = clientCacheRule.getClientCache().getRegion(name);
    for (int key = 0; key < numberOfEntries; key++) {
      txManager.begin();
      region.get(key);
      txManager.commit();
    }
  }

  private void getDataMissed(int numberOfMissedGets, String name) {
    Region<Object, Object> region = clientCacheRule.getClientCache().getRegion(name);
    for (int key = 0; key < numberOfMissedGets; key++) {
      region.get(NON_EXISTENT_KEY);
    }
  }

  private int createServerRegion() throws Exception {
    PartitionAttributesFactory<Object, Object> factory = new PartitionAttributesFactory<>();
    PartitionAttributes<Object, Object> partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionNamePartitioned);
    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private int createReplicatedServerRegion() throws Exception {
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create(regionNameReplicated);
    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int port) {
    clientCacheRule.createClientCache();
    PoolImpl pool = getPool(port);
    ClientRegionFactory<Object, Object> crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.setPoolName(pool.getName());
    crf.create(regionNamePartitioned);
    pool.acquireConnection(new ServerLocation(hostName, port));
  }

  private void createClientReplicatedRegion(int port) {
    clientCacheRule.createClientCache();
    PoolImpl pool = getPool(port);
    ClientRegionFactory<Object, Object> crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.setPoolName(pool.getName());
    crf.create(regionNameReplicated);
    pool.acquireConnection(new ServerLocation(hostName, port));
  }

  private PoolImpl getPool(int port) {
    PoolFactory factory = PoolManager.createFactory();
    factory.addServer(hostName, port);
    return (PoolImpl) factory.create(uniqueName);
  }

  private void validateGetsRates(final String regionName, final int expectedCount,
      final int expectedMissed) {
    InternalCache cache = cacheRule.getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    CachePerfStats regionPerfStats = region.getCachePerfStats();
    checkGetsStats(regionPerfStats, expectedCount, expectedMissed);
    CachePerfStats cachePerfStats = cache.getCachePerfStats();
    checkGetsStats(cachePerfStats, expectedCount, expectedMissed);
  }

  private void validateGetsRatesReplicate(final String regionName, final int expectedCount,
      final int expectedMissed) {
    InternalCache cache = cacheRule.getCache();
    DistributedRegion region = (DistributedRegion) cache.getRegion(regionName);
    CachePerfStats regionPerfStats = region.getCachePerfStats();
    checkGetsStats(regionPerfStats, expectedCount, expectedMissed);
    CachePerfStats cachePerfStats = cache.getCachePerfStats();
    checkGetsStats(cachePerfStats, expectedCount, expectedMissed);
  }

  private void checkGetsStats(CachePerfStats cachePerfStats, int expectedCount,
      int expectedMissed) {
    assertThat(cachePerfStats.getGets()).isEqualTo(expectedCount);
    assertThat(cachePerfStats.getMisses()).isEqualTo(expectedMissed);
  }
}
