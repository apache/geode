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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@SuppressWarnings("serial")
public class ClientServerTransactionFailoverWithMixedVersionServersDistributedTest
    implements Serializable {

  private static final int TRANSACTION_TIMEOUT_SECOND = 2;
  private static final int VM_COUNT = 6;

  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private VM server3;
  private VM server4;
  private VM locator;
  private VM client;
  private int locatorPort;
  private File locatorLog;
  private Host host;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(VM_COUNT);

  @Rule
  public CacheRule cacheRule = new CacheRule(VM_COUNT);

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule(VM_COUNT);

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setup() throws Exception {
    host = Host.getHost(0);
    String startingVersion = "160";
    server1 = host.getVM(startingVersion, 0);
    server2 = host.getVM(startingVersion, 1);
    server3 = host.getVM(startingVersion, 2);
    server4 = host.getVM(startingVersion, 3);
    client = host.getVM(startingVersion, 4);
    locator = host.getVM(startingVersion, 5);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    locatorLog = new File(temporaryFolder.newFolder(uniqueName), "locator.log");
  }

  @Test
  public void clientTransactionOperationsAreNotLostIfTransactionIsOnRolledServer()
      throws Exception {
    setupPartiallyRolledVersion();

    server1.invoke(() -> createServerRegion(1, false));
    server1.invoke(() -> cacheRule.getCache().getRegion(regionName).put(1, "originalValue"));
    server2.invoke(() -> createServerRegion(1, true));
    server3.invoke(() -> createServerRegion(1, true));
    server4.invoke(() -> createServerRegion(1, true));
    client.invoke(() -> createClientRegion());

    ClientProxyMembershipID clientProxyMembershipID = client.invoke(() -> getClientId());

    int numOfTransactions = 12;
    int numOfOperations = 12;
    client.invokeAsync(() -> doTransactions(numOfTransactions, numOfOperations));

    server1.invoke(() -> verifyTransactionAreStarted(numOfTransactions));

    unregisterClientMultipleTimes(clientProxyMembershipID);

    server1.invoke(() -> verifyTransactionResult(numOfTransactions, numOfOperations, false));
    client.invoke(() -> verifyTransactionResult(numOfTransactions, numOfOperations, true));
  }

  private void doTransactions(int numOfTransactions, int numOfOperations)
      throws InterruptedException, ExecutionException {
    Thread[] threads = new Thread[numOfTransactions];
    FutureTask<TransactionId>[] futureTasks = new FutureTask[numOfTransactions];
    TransactionId[] txIds = new TransactionId[numOfTransactions];
    // begin and suspend transactions
    beginAndSuspendTransactions(numOfTransactions, numOfOperations, threads, futureTasks, txIds);
    // resume transactions
    resumeTransactions(numOfTransactions, numOfOperations, threads, txIds);
    waitForResumeTransactionsToComplete(numOfTransactions, threads);
  }

  private void setupPartiallyRolledVersion() throws Exception {
    locatorPort = locator.invoke(() -> startLocator());
    server1.invoke(() -> createCacheServer());
    server2.invoke(() -> createCacheServer());
    server3.invoke(() -> createCacheServer());
    server4.invoke(() -> createCacheServer());
    client.invoke(() -> createClientCache());

    // roll locator
    locator = rollLocatorToCurrent(locator);
    // roll server1
    server1 = rollServerToCurrent(server1);
    server2 = rollServerToCurrent(server2);
  }

  private int startLocator() throws IOException {
    Properties config = createLocatorConfig();
    InetAddress bindAddress = InetAddress.getByName(hostName);
    Locator locator = Locator.startLocatorAndDS(locatorPort, locatorLog, bindAddress, config);
    return locator.getPort();
  }

  private Properties createLocatorConfig() {
    Properties config = new Properties();
    config.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    config.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return config;
  }

  private void createCacheServer() throws Exception {
    cacheRule.createCache(createServerConfig());

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
  }

  private Properties createServerConfig() {
    Properties config = createLocatorConfig();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    return config;
  }

  private void createClientCache() {
    clientCacheRule.createClientCache();
  }

  private VM rollLocatorToCurrent(VM oldLocator) {
    // Roll the locator
    oldLocator.invoke(() -> stopLocator());
    VM rollLocator = host.getVM(VersionManager.CURRENT_VERSION, oldLocator.getId());
    rollLocator.invoke(() -> startLocator());
    return rollLocator;
  }

  private void stopLocator() {
    Locator.getLocator().stop();
  }

  private VM rollServerToCurrent(VM oldServer) {
    // Roll the server
    oldServer.invoke(() -> cacheRule.getCache().close());
    VM rollServer = host.getVM(VersionManager.CURRENT_VERSION, oldServer.getId());
    rollServer.invoke(() -> createCacheServer());
    return rollServer;
  }

  private void createServerRegion(int totalNumBuckets, boolean isAccessor) {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }
    PartitionAttributes partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    txManager.setTransactionTimeToLiveForTest(TRANSACTION_TIMEOUT_SECOND);
  }

  private void createClientRegion() {
    Pool pool = PoolManager.createFactory().addLocator(hostName, locatorPort).create(uniqueName);

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);
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

  private TransactionId beginAndSuspendTransaction(int numOfOperations) {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    startTransaction(numOfOperations, region, txManager);

    return txManager.suspend();
  }

  private void startTransaction(int numOfOperations, Region region, TXManagerImpl txManager) {
    txManager.begin();
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getTXState();
    int whichTransaction = ((TXId) txStateProxy.getTransactionId()).getUniqId();
    int key = getKey(whichTransaction, numOfOperations);
    String value = getValue(key);
    region.put(key, value);
  }

  private int getKey(int whichTransaction, int numOfOperations) {
    return numOfOperations * (whichTransaction - 1) + 1;
  }

  private String getValue(int key) {
    return "value" + key;
  }

  private ClientProxyMembershipID getClientId() {
    DistributedMember distributedMember =
        clientCacheRule.getClientCache().getInternalDistributedSystem().getDistributedMember();
    return ClientProxyMembershipID.getClientId(distributedMember);
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

  private void resumeTransaction(TransactionId txId, int numOfOperations) throws Exception {
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

  private void unregisterClientMultipleTimes(ClientProxyMembershipID clientProxyMembershipID)
      throws Exception {
    int numOfUnregisterClients = 4;
    for (int i = 0; i < numOfUnregisterClients; i++) {
      getVM(i).invoke(() -> unregisterClient(clientProxyMembershipID));
      Thread.sleep(1000);
    }
  }

  private void unregisterClient(ClientProxyMembershipID clientProxyMembershipID) {
    ClientHealthMonitor clientHealthMonitor = ClientHealthMonitor.getInstance();
    clientHealthMonitor.removeAllConnectionsAndUnregisterClient(clientProxyMembershipID,
        new Exception());
  }

  private void waitForResumeTransactionsToComplete(int numOfTransactions, Thread[] threads)
      throws InterruptedException {
    for (int i = 0; i < numOfTransactions; i++) {
      threads[i].join();
    }
  }

  private void verifyTransactionResult(int numOfTransactions, int numOfOperations,
      boolean isClient) {
    Region region;
    if (isClient) {
      region = clientCacheRule.getClientCache().getRegion(regionName);
    } else {
      region = cacheRule.getCache().getRegion(regionName);
    }
    int numOfEntries = numOfOperations * numOfTransactions;
    await()
        .untilAsserted(() -> assertThat(region.size()).isEqualTo(numOfEntries));
    for (int i = 1; i <= numOfEntries; i++) {
      LogService.getLogger().info("region get key {} value {} ", i, region.get(i));
    }
    for (int i = 1; i <= numOfEntries; i++) {
      assertEquals("value" + i, region.get(i));
    }
  }

  @Test
  public void clientTransactionExpiredAreRemovedOnRolledServer() throws Exception {
    setupPartiallyRolledVersion();

    server1.invoke(() -> createServerRegion(1, false));
    server2.invoke(() -> createServerRegion(1, true));
    server3.invoke(() -> createServerRegion(1, true));
    server4.invoke(() -> createServerRegion(1, true));
    client.invoke(() -> createClientRegion());

    ClientProxyMembershipID clientProxyMembershipID = client.invoke(() -> getClientId());

    int numOfTransactions = 12;
    int numOfOperations = 1;
    client.invokeAsync(() -> doUnfinishedTransactions(numOfTransactions, numOfOperations));

    server1.invoke(() -> verifyTransactionAreStarted(numOfTransactions));

    unregisterClientMultipleTimes(clientProxyMembershipID);

    server1.invoke(() -> verifyTransactionAreExpired());
  }

  private void doUnfinishedTransactions(int numOfTransactions, int numOfOperations)
      throws InterruptedException {
    Thread[] threads = new Thread[numOfTransactions];
    for (int i = 0; i < numOfTransactions; i++) {
      Thread thread = new Thread(() -> startTransaction(numOfOperations));
      threads[i] = thread;
      thread.start();
    }

    for (int i = 0; i < numOfTransactions; i++) {
      threads[i].join();
    }
  }

  private void startTransaction(int numOfOperations) {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    startTransaction(numOfOperations, region, txManager);
  }

  private void verifyTransactionAreStarted(int numOfTransactions) {
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    await()
        .untilAsserted(() -> assertThat(txManager.hostedTransactionsInProgressForTest())
            .isEqualTo(numOfTransactions));
  }

  private void verifyTransactionAreExpired() {
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    await()
        .untilAsserted(
            () -> assertThat(txManager.hostedTransactionsInProgressForTest()).isEqualTo(0));
  }
}
