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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings("serial")
public class TransactionOnBehalfOfClientMemberDistributedTest implements Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;
  private int port1;

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
  public void clientTransactionShouldSetOnBehalfOfClientMember() {
    port1 = server1.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> createServerRegion(1, false));
    createClientRegion(port1);

    TransactionId txId = beginAndSuspendTransaction(true);
    server1.invoke(() -> verifyOnBehalfOfClientMember(true));
    server2.invoke(() -> verifyOnBehalfOfClientMember(true));
    resumeAndCommitTransaction(true, txId);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor) throws IOException {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    txManager.setTransactionTimeToLiveForTest(4);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int port) {
    clientCacheRule.createClientCache();
    Pool pool = PoolManager.createFactory().addServer(hostName, port).create(uniqueName);

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);
  }

  private TransactionId beginAndSuspendTransaction(boolean isClient) {
    Region region = getRegion(isClient);
    TXManagerImpl txManager = getTXManager(isClient);
    txManager.begin();
    region.put(1, "value1");
    return txManager.suspend();
  }

  private Region getRegion(boolean isClient) {
    if (isClient) {
      return clientCacheRule.getClientCache().getRegion(regionName);
    }
    return cacheRule.getCache().getRegion(regionName);
  }

  private TXManagerImpl getTXManager(boolean isClient) {
    if (isClient) {
      return (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    }
    return cacheRule.getCache().getTxManager();
  }

  private void verifyOnBehalfOfClientMember(boolean shouldSet) {
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    ArrayList<TXId> txIds = txManager.getHostedTxIds();
    for (TXId txId : txIds) {
      TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getHostedTXState(txId);
      if (shouldSet) {
        assertTrue(txStateProxy.isOnBehalfOfClient());
      } else {
        assertFalse(txStateProxy.isOnBehalfOfClient());
      }
    }
  }

  private void resumeAndCommitTransaction(boolean isClient, TransactionId txId) {
    TXManagerImpl txManager = getTXManager(isClient);
    txManager.resume(txId);
    txManager.commit();
  }

  @Test
  public void peerTransactionShouldNotSetOnBehalfOfClientMember() {
    port1 = server1.invoke(() -> createServerRegion(1, true));
    server2.invoke(() -> createServerRegion(1, false));
    createClientRegion(port1);

    TransactionId txId = server1.invoke(() -> beginAndSuspendTransaction(false));
    server1.invoke(() -> verifyOnBehalfOfClientMember(false));
    server2.invoke(() -> verifyOnBehalfOfClientMember(false));
    server1.invoke(() -> resumeAndCommitTransaction(false, txId));
  }
}
