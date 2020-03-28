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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase.getBlackboard;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheTransactionManager;
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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings("serial")
public class ClientServerTransactionFailoverDistributedTest implements Serializable {

  private final int numOfOperationsInTransaction = 10;
  private final int key1 = 1;
  private final String originalValue = "originalValue";

  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private VM server3;
  private VM server4;
  private int port1;
  private int port2;

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
  public void clientLongTransactionDoesNotLoseOperationsAfterFailoverDirectlyToTransactionHost()
      throws Exception {
    port1 = server1.invoke(() -> createServerRegion(1, false));
    server1.invoke(() -> doPut(key1, originalValue));
    port2 = server2.invoke(() -> createServerRegion(1, true));

    int numOfOpertions = 5;
    createClientRegion(true, port2, port1);
    TransactionId txId = beginAndSuspendTransaction(numOfOpertions);
    server2.invoke(() -> {
      cacheRule.getCache().close();
    });
    resumeTransaction(txId, numOfOpertions);

    server1.invoke(() -> verifyTransactionResult(1, numOfOpertions));
  }

  private void doPut(int key, String value) {
    cacheRule.getCache().getRegion(regionName).put(key, value);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor) throws Exception {
    return createServerRegion(totalNumBuckets, isAccessor, 0);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor, int redundancy)
      throws Exception {
    return createServerRegion(totalNumBuckets, isAccessor, redundancy, false);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor, int redundancy,
      boolean setMaximumTimeBetweenPingsLargerThanDefaultTimeout)
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
    if (setMaximumTimeBetweenPingsLargerThanDefaultTimeout) {
      // set longer than GeodeAwaitility.DEFAULT_TIMEOUT to avoid GII triggered by
      // region synchronize with.
      // This ensures the commit is brought into replicas by the CommitProcessQueryMessage.
      server.setMaximumTimeBetweenPings((int) GeodeAwaitility.getTimeout().toMillis() + 60000);
    }
    server.start();
    return server.getPort();
  }

  private void createClientRegion(boolean connectToFirstPort, int... ports) {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl pool;
    try {
      pool = getPool(ports);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);

    if (ports.length > 1 && connectToFirstPort) {
      // first connection to the first port in the list
      pool.acquireConnection(new ServerLocation(hostName, ports[0]));
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

  private TransactionId beginAndSuspendTransaction(int numOfOperations) {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();

    txManager.begin();
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    int whichTransaction = ((TXId) txStateProxy.getTransactionId()).getUniqId();
    int key = getKey(whichTransaction, numOfOperations);
    String value = getValue(key);
    region.put(key, value);

    return txManager.suspend();
  }

  private void resumeTransaction(TransactionId txId, int numOfOperations)
      throws InterruptedException {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.resume(txId);
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    int whichTransaction = ((TXId) txStateProxy.getTransactionId()).getUniqId();

    int initialKey = getKey(whichTransaction, numOfOperations);
    int key = 0;
    for (int i = 0; i < numOfOperations; i++) {
      key = initialKey + i;
      String value = getValue(key);
      region.put(key, value);
      Thread.sleep(1000);
    }
    txManager.commit();
  }

  private void verifyTransactionResult(int numOfTransactions, int numOfOperations) {
    Region region = cacheRule.getCache().getRegion(regionName);
    int numOfEntries = numOfOperations * numOfTransactions;
    for (int i = 1; i <= numOfEntries; i++) {
      LogService.getLogger().info("region get key {} value {} ", i, region.get(i));
    }
    for (int i = 1; i <= numOfEntries; i++) {
      assertEquals("value" + i, region.get(i));
    }
  }

  private ClientProxyMembershipID getClientId() {
    DistributedMember distributedMember =
        clientCacheRule.getClientCache().getInternalDistributedSystem().getDistributedMember();
    return ClientProxyMembershipID.getClientId(distributedMember);
  }

  private void unregisterClient(ClientProxyMembershipID clientProxyMembershipID) throws Exception {
    ClientHealthMonitor clientHealthMonitor = ClientHealthMonitor.getInstance();
    clientHealthMonitor.removeAllConnectionsAndUnregisterClient(clientProxyMembershipID,
        new Exception());
  }

  private int getKey(int whichTransaction, int numOfOperations) {
    return numOfOperations * (whichTransaction - 1) + 1;
  }

  private String getValue(int key) {
    return "value" + key;
  }

  @Test
  public void multipleClientLongTransactionsCanFailoverWithoutLosingOperations() throws Exception {
    // set up
    setupClientAndServerForMultipleTransactions();

    int numOfTransactions = 12;
    Thread[] threads = new Thread[numOfTransactions];
    FutureTask<TransactionId>[] futureTasks = new FutureTask[numOfTransactions];
    TransactionId[] txIds = new TransactionId[numOfTransactions];

    // begin and suspend transactions
    beginAndSuspendTransactions(numOfTransactions, numOfOperationsInTransaction, threads,
        futureTasks,
        txIds);

    // unregister client on 2 of the servers
    ClientProxyMembershipID clientProxyMembershipID = getClientId();
    server1.invoke(() -> unregisterClient(clientProxyMembershipID));
    server2.invoke(() -> unregisterClient(clientProxyMembershipID));

    resumeTransactions(numOfTransactions, numOfOperationsInTransaction, threads, txIds);
    waitForResumeTransactionsToComplete(numOfTransactions, threads);

    server4.invoke(() -> verifyTransactionResult(numOfTransactions, numOfOperationsInTransaction));
  }

  @Test
  public void multipleClientLongTransactionsCanFailoverMultipleTimesWithoutLosingOperations()
      throws Exception {
    // set up
    setupClientAndServerForMultipleTransactions();

    int numOfTransactions = 12;
    int numOfOperations = 12;
    Thread[] threads = new Thread[numOfTransactions];
    FutureTask<TransactionId>[] futureTasks = new FutureTask[numOfTransactions];
    TransactionId[] txIds = new TransactionId[numOfTransactions];

    // begin and suspend transactions
    beginAndSuspendTransactions(numOfTransactions, numOfOperations, threads, futureTasks, txIds);

    // unregister client multiple times
    ClientProxyMembershipID clientProxyMembershipID = getClientId();

    resumeTransactions(numOfTransactions, numOfOperations, threads, txIds);
    unregisterClientMultipleTimes(clientProxyMembershipID);
    waitForResumeTransactionsToComplete(numOfTransactions, threads);

    server4.invoke(() -> verifyTransactionResult(numOfTransactions, numOfOperations));
  }

  private void waitForResumeTransactionsToComplete(int numOfTransactions, Thread[] threads)
      throws InterruptedException {
    for (int i = 0; i < numOfTransactions; i++) {
      threads[i].join();
    }
  }

  private void beginAndSuspendTransactions(int numOfTransactions, int numOfOperations,
      Thread[] threads,
      FutureTask<TransactionId>[] futureTasks, TransactionId[] txIds)
      throws InterruptedException, ExecutionException {
    for (int i = 0; i < numOfTransactions; i++) {
      FutureTask<TransactionId> futureTask =
          new FutureTask<>(() -> beginAndSuspendTransaction(numOfOperations));
      futureTasks[i] = futureTask;
      Thread thread = new Thread(futureTask);
      threads[i] = thread;
      thread.start();
    }

    for (int i = 0; i < numOfTransactions; i++) {
      txIds[i] = futureTasks[i].get();
    }
  }

  private void setupClientAndServerForMultipleTransactions() {
    int port4 = server4.invoke(() -> createServerRegion(1, false));
    server4.invoke(() -> doPut(key1, originalValue));
    port1 = server1.invoke(() -> createServerRegion(1, true));
    port2 = server2.invoke(() -> createServerRegion(1, true));
    int port3 = server3.invoke(() -> createServerRegion(1, true));
    createClientRegion(false, port1, port2, port3, port4);
  }

  private void resumeTransactions(int numOfTransactions, int numOfOperations, Thread[] threads,
      TransactionId[] txIds) {
    for (int i = 0; i < numOfTransactions; i++) {
      TransactionId txId = txIds[i];
      Thread thread = new Thread(() -> {
        try {
          resumeTransaction(txId, numOfOperations);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      thread.start();
      threads[i] = thread;
    }
  }

  private void unregisterClientMultipleTimes(ClientProxyMembershipID clientProxyMembershipID)
      throws InterruptedException {
    int numOfUnregisterClients = 4;
    for (int i = 0; i < numOfUnregisterClients; i++) {
      getVM(i).invoke(() -> unregisterClient(clientProxyMembershipID));
      Thread.sleep(1000);
    }
  }

  @Test
  public void txCommitGetsAppliedOnAllTheReplicasAfterHostIsShutDownAndIfOneOfTheNodeHasCommitted()
      throws Exception {
    getBlackboard().initBlackboard();
    VM client = server4;

    port1 = server1.invoke(() -> createServerRegion(1, false, 2, true));

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      region.put("Key-1", "Value-1");
      region.put("Key-2", "Value-2");
    });

    port2 = server2.invoke(() -> createServerRegion(1, false, 2, true));

    server3.invoke(() -> createServerRegion(1, false, 2, true));

    client.invoke(() -> createClientRegion(true, port1, port2));

    server1.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new DistributionMessageObserver() {
            @Override
            public void beforeSendMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              if (message instanceof TXCommitMessage.CommitProcessForTXIdMessage) {
                InternalDistributedMember m = message.getRecipients().get(0);
                message.resetRecipients();
                message.setRecipient(m);
              }
            }
          });
    });

    server2.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new DistributionMessageObserver() {
            @Override
            public void beforeProcessMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              if (message instanceof TXCommitMessage.CommitProcessForTXIdMessage) {
                getBlackboard().signalGate("bounce");
              }
            }
          });
    });

    server3.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new DistributionMessageObserver() {
            @Override
            public void beforeProcessMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              if (message instanceof TXCommitMessage.CommitProcessForTXIdMessage) {
                getBlackboard().signalGate("bounce");
              }
            }
          });
    });

    AsyncInvocation clientAsync = client.invokeAsync(() -> {
      {
        CacheTransactionManager transactionManager =
            clientCacheRule.getClientCache().getCacheTransactionManager();
        Region region = clientCacheRule.getClientCache().getRegion(regionName);
        transactionManager.begin();
        region.put("TxKey-1", "TxValue-1");
        region.put("TxKey-2", "TxValue-2");
        transactionManager.commit();
      }
    });

    await().until(() -> getBlackboard().isGateSignaled("bounce"));
    server1.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
    server1.bounceForcibly();

    clientAsync.join();

    server2.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      await().untilAsserted(() -> {
        assertThat(region.get("TxKey-1")).isEqualTo("TxValue-1");
        assertThat(region.get("TxKey-2")).isEqualTo("TxValue-2");
      });
    });

    server3.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      await().untilAsserted(() -> {
        assertThat(region.get("TxKey-1")).isEqualTo("TxValue-1");
        assertThat(region.get("TxKey-2")).isEqualTo("TxValue-2");
      });
    });

    client.invoke(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      assertThat(region.get("TxKey-1")).isEqualTo("TxValue-1");
      assertThat(region.get("TxKey-2")).isEqualTo("TxValue-2");
    });

    server2.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
    server3.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
  }
}
