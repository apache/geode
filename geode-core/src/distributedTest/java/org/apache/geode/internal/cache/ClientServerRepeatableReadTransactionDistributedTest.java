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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class ClientServerRepeatableReadTransactionDistributedTest implements Serializable {
  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
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

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
  }

  @Test
  public void valuesRepeatableReadDoesNotIncludeTombstones() {
    port1 = server1.invoke(() -> createServerRegion(1));

    createClientRegion(port1);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    region.put("key1", "value1");
    region.destroy("key1"); // creates a tombstone

    TXManagerImpl txMgr =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    Region region1 = clientCacheRule.getClientCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region1.values().toArray(); // this is a repeatable read
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region1.put("key1", "newValue");
    txMgr.commit();

    txMgr.resume(txId);
    region1.put("key1", "value1");
    txMgr.commit();
    assertThat(region.get("key1")).isEqualTo("value1");
  }

  @Test
  public void keySetRepeatableReadDoesNotIncludeTombstones() {
    port1 = server1.invoke(() -> createServerRegion(1));

    createClientRegion(port1);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    region.put("key1", "value1");
    region.destroy("key1"); // creates a tombstone

    TXManagerImpl txMgr =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    Region region1 = clientCacheRule.getClientCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region1.keySet().toArray(); // this is a repeatable read
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region1.put("key1", "newValue");
    txMgr.commit();

    txMgr.resume(txId);
    region1.put("key1", "value1");
    txMgr.commit();
    assertThat(region.get("key1")).isEqualTo("value1");
  }

  @Test
  public void valuesRepeatableReadIncludesInvalidates() {
    port1 = server1.invoke(() -> createServerRegion(1));

    createClientRegion(port1);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    region.put("key1", "value1");
    region.invalidate("key1");

    TXManagerImpl txMgr =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    Region region1 = clientCacheRule.getClientCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region1.values().toArray(); // this is a repeatable read
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region1.put("key1", "newValue");
    txMgr.commit();

    txMgr.resume(txId);
    region1.put("key1", "value1");
    assertThatThrownBy(txMgr::commit).isExactlyInstanceOf(CommitConflictException.class);
    assertThat(region.get("key1")).isEqualTo("newValue");
  }

  @Test
  public void keySetRepeatableReadIncludesInvalidates() {
    port1 = server1.invoke(() -> createServerRegion(1));

    createClientRegion(port1);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    region.put("key1", "value1");
    region.invalidate("key1");

    TXManagerImpl txMgr =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    Region region1 = clientCacheRule.getClientCache().getRegion(regionName);
    txMgr.begin(); // tx1
    region1.keySet().toArray(); // this is a repeatable read
    TransactionId txId = txMgr.suspend();

    txMgr.begin(); // tx2
    region1.put("key1", "newValue");
    txMgr.commit();

    txMgr.resume(txId);
    region1.put("key1", "value1");
    assertThatThrownBy(txMgr::commit).isExactlyInstanceOf(CommitConflictException.class);
    assertThat(region.get("key1")).isEqualTo("newValue");
  }

  private int createServerRegion(int totalNumBuckets) throws Exception {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets);
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int port) {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl pool;
    try {
      pool = getPool(port);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);
  }

  private PoolImpl getPool(int port) {
    PoolFactory factory = PoolManager.createFactory();
    factory.addServer(hostName, port);
    factory.setReadTimeout(12000).setSocketBufferSize(1000);
    return (PoolImpl) factory.create(uniqueName);
  }

}
