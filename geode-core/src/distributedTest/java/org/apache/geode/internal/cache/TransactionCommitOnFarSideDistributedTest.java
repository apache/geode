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
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class TransactionCommitOnFarSideDistributedTest implements Serializable {
  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private VM server3;
  private VM server4;
  private int port1;
  private final String key = "key";
  private final String value = "value";
  private final String newValue = "newValue";

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
    server3 = getVM(2);
    server4 = getVM(3);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
  }

  @Test
  public void farSideFailoverMapDoesNotSaveTransactionsInitiatedFromServer() {
    port1 = server1.invoke(() -> createServerRegion(1, false, 2));
    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      region.put(key, value);
    });
    server2.invoke(() -> createServerRegion(1, false, 2));
    server3.invoke(() -> createServerRegion(1, false, 2));

    server1.invoke(() -> {
      TXManagerImpl txManager = cacheRule.getCache().getTxManager();
      txManager.begin();
      Region region = cacheRule.getCache().getRegion(regionName);
      region.put(key, newValue);
      txManager.commit();
      assertThat(region.get(key)).isEqualTo(newValue);
      assertThat(txManager.getFailoverMapSize()).isZero();
    });
    server2.invoke(() -> verifyFarSideFailoverMapSizeAfterCommit(0));
    server3.invoke(() -> verifyFarSideFailoverMapSizeAfterCommit(0));
  }

  private void verifyFarSideFailoverMapSizeAfterCommit(int expectedValue) {
    Region region = cacheRule.getCache().getRegion(regionName);
    assertThat(region.get(key)).isEqualTo(newValue);
    assertThat(TXCommitMessage.getTracker().getFailoverMapSize()).isEqualTo(expectedValue);
  }

  @Test
  public void ensureBucketSizeDoesNotGoNegative_whenTxWithDeltaAndSizeable() {
    server1.invoke(() -> createServerRegion(1, false, 0));

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      CacheTransactionManager txManager = cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      region.put("key1", new TestDeltaSerializableSizeableObject("small value"));
      txManager.commit();
      TestDeltaSerializableSizeableObject testClass =
          (TestDeltaSerializableSizeableObject) region.get("key1");
      testClass.value = "some value that is much larger than the initial value";
      region.put("key1", testClass);
      region.destroy("key1");
    });
  }

  @Test
  public void farSideFailoverMapSavesTransactionsInitiatedFromClient() {
    VM client = server4;
    port1 = server1.invoke(() -> createServerRegion(1, false, 2));
    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      region.put(key, value);
    });
    server2.invoke(() -> createServerRegion(1, false, 2));
    server3.invoke(() -> createServerRegion(1, false, 2));

    client.invoke(() -> createClientRegion(true, port1));
    client.invoke(() -> {
      CacheTransactionManager transactionManager =
          clientCacheRule.getClientCache().getCacheTransactionManager();
      transactionManager.begin();
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      region.put(key, newValue);
      transactionManager.commit();
    });

    server1.invoke(() -> {
      TXManagerImpl txManager = cacheRule.getCache().getTxManager();
      Region region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get(key)).isEqualTo(newValue);
      assertThat(txManager.getFailoverMapSize()).isEqualTo(1);
    });
    server2.invoke(() -> verifyFarSideFailoverMapSizeAfterCommit(1));
    server3.invoke(() -> verifyFarSideFailoverMapSizeAfterCommit(1));
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

  private void createClientRegion(boolean connectToFirstPort, int... ports) {
    clientCacheRule.createClientCache();

    PoolImpl pool = getPool(ports);
    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);

  }

  private PoolImpl getPool(int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setReadTimeout(12000).setSocketBufferSize(1000);

    return (PoolImpl) factory.create(uniqueName);
  }
}
