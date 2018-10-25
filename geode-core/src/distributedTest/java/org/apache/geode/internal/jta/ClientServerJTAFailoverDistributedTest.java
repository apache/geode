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

package org.apache.geode.internal.jta;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import javax.transaction.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PeerTXStateStub;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings("serial")
public class ClientServerJTAFailoverDistributedTest implements Serializable {

  private final int key = 1;
  private final String value = "value1";
  private final String newValue = "value2";

  private String hostName;
  private String uniqueName;
  private String regionName;
  private String replicateRegionName;
  private VM server1;
  private VM server2;
  private VM server3;
  private VM client1;
  private int port1;
  private int port2;
  private boolean hasReplicateRegion = false;

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
    client1 = getVM(3);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    replicateRegionName = uniqueName + "_replicate_region";
  }

  @Test
  public void jtaCanFailoverAfterDoneBeforeCompletion() {
    server3.invoke(() -> createServerRegion(1, false));
    server3.invoke(() -> doPut(key, value));
    port1 = server1.invoke(() -> createServerRegion(1, true));
    port2 = server2.invoke(() -> createServerRegion(1, true));

    client1.invoke(() -> createClientRegion(port1, port2));

    Object[] beforeCompletionResults = client1.invoke(() -> doBeforeCompletion());

    int port = (Integer) beforeCompletionResults[1];

    if (port == port1) {
      server1.invoke(() -> cacheRule.getCache().close());
    } else {
      assert port == port2;
      server2.invoke(() -> cacheRule.getCache().close());
    }

    client1.invoke(() -> doAfterCompletion((TransactionId) beforeCompletionResults[0], true));
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor) throws Exception {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
    if (isAccessor) {
      partitionAttributesFactory.setLocalMaxMemory(0);
    }

    RegionFactory regionFactory = cacheRule.getOrCreateCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create(regionName);

    if (hasReplicateRegion) {
      cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.REPLICATE)
          .create(replicateRegionName);
    }


    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int... ports) {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl pool;
    try {
      pool = getPool(ports);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    ClientRegionFactory<?, ?> clientRegionFactory =
        clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    clientRegionFactory.setPoolName(pool.getName());
    clientRegionFactory.create(regionName);

    if (hasReplicateRegion) {
      clientRegionFactory.create(replicateRegionName);
    }

    if (ports.length > 1) {
      pool.acquireConnection(new ServerLocation(hostName, port1));
    }
  }

  private PoolImpl getPool(int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    return (PoolImpl) factory.setReadTimeout(2000).setSocketBufferSize(1000)
        .setMinConnections(4).create(uniqueName);
  }

  private void doPut(int key, String value) {
    cacheRule.getCache().getRegion(regionName).put(key, value);
  }

  private Object[] doBeforeCompletion() {
    Object[] results = new Object[2];
    InternalClientCache cache = clientCacheRule.getClientCache();
    Region region = cache.getRegion(regionName);
    Region replicateRegion = hasReplicateRegion ? cache.getRegion(replicateRegionName) : null;
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    region.put(key, newValue);
    if (hasReplicateRegion) {
      replicateRegion.put(key, newValue);
    }
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    ClientTXStateStub clientTXStateStub = (ClientTXStateStub) txStateProxy.getRealDeal(null, null);
    clientTXStateStub.beforeCompletion();
    TransactionId transactionId = txManager.suspend();
    int port = clientTXStateStub.getServerAffinityLocation().getPort();
    results[0] = transactionId;
    results[1] = port;
    return results;
  }

  private void doAfterCompletion(TransactionId transactionId, boolean isCommit) {
    InternalClientCache cache = clientCacheRule.getClientCache();
    Region region = cache.getRegion(regionName);
    Region replicateRegion = cache.getRegion(replicateRegionName);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.resume(transactionId);

    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    ClientTXStateStub clientTXStateStub = (ClientTXStateStub) txStateProxy.getRealDeal(null, null);
    try {
      clientTXStateStub
          .afterCompletion(isCommit ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK);
    } catch (Exception exception) {
      LogService.getLogger().info("exception stack ", exception);
      throw exception;
    }
    if (isCommit) {
      assertEquals(newValue, region.get(key));
      if (hasReplicateRegion) {
        assertEquals(newValue, replicateRegion.get(key));
      }
    } else {
      assertEquals(value, region.get(key));
    }
  }

  @Test
  public void jtaCanFailoverToJTAHostAfterDoneBeforeCompletion() {
    port2 = server2.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> doPut(key, value));
    port1 = server1.invoke(() -> createServerRegion(1, true));

    client1.invoke(() -> createClientRegion(port1, port2));
    Object[] beforeCompletionResults = client1.invoke(() -> doBeforeCompletion());

    server1.invoke(() -> cacheRule.getCache().close());

    client1.invoke(() -> doAfterCompletion((TransactionId) beforeCompletionResults[0], true));
  }

  @Test
  public void jtaCanFailoverWithRollbackAfterDoneBeforeCompletion() {
    server3.invoke(() -> createServerRegion(1, false));
    server3.invoke(() -> doPut(key, value));
    port1 = server1.invoke(() -> createServerRegion(1, true));
    port2 = server2.invoke(() -> createServerRegion(1, true));

    client1.invoke(() -> createClientRegion(port1, port2));

    Object[] beforeCompletionResults = client1.invoke(() -> doBeforeCompletion());

    int port = (Integer) beforeCompletionResults[1];

    if (port == port1) {
      server1.invoke(() -> cacheRule.getCache().close());
    } else {
      assert port == port2;
      server2.invoke(() -> cacheRule.getCache().close());
    }

    client1.invoke(() -> doAfterCompletion((TransactionId) beforeCompletionResults[0], false));

    createClientRegion(port == port1 ? port2 : port1);
    doPutTransaction(true);
  }

  private void doPutTransaction(boolean isClient) {
    Region region;
    TXManagerImpl txManager;
    if (isClient) {
      InternalClientCache cache = clientCacheRule.getClientCache();
      region = cache.getRegion(regionName);
      txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    } else {
      InternalCache cache = cacheRule.getCache();
      region = cache.getRegion(regionName);
      txManager = (TXManagerImpl) cache.getCacheTransactionManager();

      await()
          .until(() -> txManager.isHostedTXStatesEmpty());
    }
    txManager.begin();
    region.put(key, newValue);
    txManager.commit();
    assertEquals(newValue, region.get(key));
  }

  @Test
  public void locksHeldInBeforeCompletionCanBeReleaseIfOriginatorDeparted() {
    server1.invoke(() -> createServerRegion(1, false));
    server1.invoke(() -> doPut(key, value));
    server2.invoke(() -> createServerRegion(1, true));

    server2.invoke(() -> doBeforeCompletionOnPeer());
    server2.invoke(() -> cacheRule.getCache().close());

    server1.invoke(() -> doPutTransaction(false));
  }

  private void doBeforeCompletionOnPeer() {
    InternalCache cache = cacheRule.getCache();
    Region region = cache.getRegion(regionName);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    region.put(key, newValue);

    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    PeerTXStateStub txStateStub = (PeerTXStateStub) txStateProxy.getRealDeal(null, null);
    txStateStub.beforeCompletion();
  }

  @Test
  public void jtaCanFailoverToJTAHostForMixedRegionsAfterDoneBeforeCompletion() {
    hasReplicateRegion = true;
    port2 = server2.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> doPut(key, value));
    port1 = server1.invoke(() -> createServerRegion(1, true));

    client1.invoke(() -> createClientRegion(port1, port2));
    Object[] beforeCompletionResults = client1.invoke(() -> doBeforeCompletion());

    server1.invoke(() -> cacheRule.getCache().close());

    client1.invoke(() -> doAfterCompletion((TransactionId) beforeCompletionResults[0], true));
  }
}
