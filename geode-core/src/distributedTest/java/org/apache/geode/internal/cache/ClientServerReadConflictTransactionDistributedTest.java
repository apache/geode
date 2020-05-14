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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheTransactionManager;
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
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.util.internal.GeodeGlossary;

public class ClientServerReadConflictTransactionDistributedTest implements Serializable {
  private static volatile DUnitBlackboard blackboard;
  private static final int key1 = 1;
  private static final int key2 = 2;
  private static final int key3 = 3;
  private static final String value1 = "value1";
  private static final String value2 = "value2";
  private static final String value3 = "value3";
  private static final String newValue1 = "newValue1";
  private static final String newValue3 = "newValue3";
  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();
  private static final String allowReadTransactionCommitToProceed =
      "allowReadTransactionCommitToProceed";
  private static final String allowSecondTransactionToProceed = "allowSecondTransactionToProceed";

  private String hostName;
  private String uniqueName;
  private String regionName;
  private String regionName2;
  private VM server1;
  private VM server2;
  private VM client1;
  private VM client2;
  private int port1;
  private int port2;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    invokeInEveryVM(() -> {
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "detectReadConflicts", "true");
    });
    server1 = getVM(0);
    server2 = getVM(1);
    client1 = getVM(2);
    client2 = getVM(3);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    regionName2 = uniqueName + "_region2";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      if (blackboard != null) {
        blackboard.clearGate(allowReadTransactionCommitToProceed);
        blackboard.clearGate(allowSecondTransactionToProceed);
      }
    });
  }

  @Test
  public void readTransactionCanBlockWriteTransactionOnPartitionedRegion() {
    createPRRegionOnServers();
    createRegionsOnClient(false);

    TransactionId readTXId = client1.invoke(() -> doReadTransaction());
    server1.invoke(() -> setAfterReservationForReadTransaction());
    client1.invokeAsync(() -> commitReadTransaction(readTXId));

    client2.invoke(() -> doPutOnReadKeyTransaction(true));
    client1.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client2.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client1.invoke(() -> verifyClientResults(regionName, key1, value1));
    client2.invoke(() -> verifyClientResults(regionName, key1, value1));

  }

  @Test
  public void readTransactionDoesNotBlockReadTransactionOnPartitionedRegion() {
    createPRRegionOnServers();
    createRegionsOnClient(false);

    TransactionId readTXId = client1.invoke(() -> doReadTransaction());
    server1.invoke(() -> setAfterReservationForReadTransaction());
    client1.invokeAsync(() -> commitReadTransaction(readTXId));

    client2.invoke(() -> doSecondReadTransaction());
    client1.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client2.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client1.invoke(() -> verifyClientResults(regionName, key1, value1));
    client2.invoke(() -> verifyClientResults(regionName, key1, value1));
  }

  private void createPRRegionOnServers() {
    port1 = server1.invoke(() -> createServerPRRegion(2));
    server1.invoke(() -> {
      // make sure key1 is on server1.
      cacheRule.getCache().getRegion(regionName).put(key1, value1);
    });
    port2 = server2.invoke(() -> createServerPRRegion(2));
    server2.invoke(() -> {
      cacheRule.getCache().getRegion(regionName).put(key2, value2);
      cacheRule.getCache().getRegion(regionName).put(key3, value3);
    });
  }

  private void createRegionsOnClient(boolean createBothRegions) {
    client1.invoke(() -> createClientRegions(createBothRegions, port1));
    client2.invoke(() -> createClientRegions(createBothRegions, port2));

    client1.invoke(() -> getAndVerifyOriginalData());
    client2.invoke(() -> getAndVerifyOriginalData());
  }

  private int createServerPRRegion(int totalNumBuckets) throws Exception {
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

  private void createClientRegions(boolean createBothRegions, int... ports) {
    clientCacheRule.createClientCache();

    PoolImpl pool;
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setSubscriptionEnabled(true).setReadTimeout(12000).setSocketBufferSize(1000);

    pool = (PoolImpl) factory.create(uniqueName);

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    Region region = crf.create(regionName);
    region.registerInterest("ALL_KEYS");
    if (createBothRegions) {
      Region region2 = crf.create(regionName2);
      region2.registerInterest("ALL_KEYS");
    }
  }

  private void getAndVerifyOriginalData() {
    assertThat(clientCacheRule.getClientCache().getRegion(regionName).get(key1)).isEqualTo(value1);
    assertThat(clientCacheRule.getClientCache().getRegion(regionName).get(key2)).isEqualTo(value2);
    assertThat(clientCacheRule.getClientCache().getRegion(regionName).get(key3)).isEqualTo(value3);
  }

  private TransactionId doReadTransaction() {
    Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    assertThat(region.get(key1)).isEqualTo(value1);
    region.put(key3, newValue3);
    return txManager.suspend();
  }

  private void setAfterReservationForReadTransaction() {
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    ArrayList<TXId> txIds = txManager.getHostedTxIds();
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) txManager.getHostedTXState(txIds.get(0));
    TXState txState = (TXState) txStateProxy.getRealDeal(null, null);
    txState.setAfterReservation(() -> readTransactionAfterReservation());
  }

  private void readTransactionAfterReservation() {
    getBlackboard().signalGate(allowSecondTransactionToProceed);
    try {
      getBlackboard().waitForGate(allowReadTransactionCommitToProceed, TIMEOUT_MILLIS,
          MILLISECONDS);
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void commitReadTransaction(TransactionId readTXId) {
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.resume(readTXId);
    txManager.commit();
  }

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  private void verifyClientResults(String regionName, int key, String expectedValue) {
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    await("Awaiting transaction to be committed")
        .untilAsserted(() -> assertThat(region.get(key)).isEqualTo(expectedValue));
  }

  private void doSecondReadTransaction() {
    try {
      getBlackboard().waitForGate(allowSecondTransactionToProceed, TIMEOUT_MILLIS, MILLISECONDS);
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      TXManagerImpl txManager =
          (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
      txManager.begin();
      assertThat(region.get(key1)).isEqualTo(value1);
      txManager.commit();
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      getBlackboard().signalGate(allowReadTransactionCommitToProceed);
    }
  }

  @Test
  public void readConflictsTransactionCanDetectStateChangeOnPartitionedRegion() {
    createPRRegionOnServers();
    createRegionsOnClient(false);

    client2.invokeAsync(() -> doPutTransaction());
    client1.invoke(() -> doReadKeyDetectStateChangeTransaction());
    client1.invoke(() -> verifyClientResults(regionName, key1, newValue1));
    client2.invoke(() -> verifyClientResults(regionName, key1, newValue1));
  }

  private void doPutTransaction() {
    try {
      getBlackboard().waitForGate(allowSecondTransactionToProceed, TIMEOUT_MILLIS, MILLISECONDS);
      Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
      TXManagerImpl txManager =
          (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
      txManager.begin();
      region.put(key1, newValue1);
      txManager.commit();
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      getBlackboard().signalGate(allowReadTransactionCommitToProceed);
    }
  }

  private void doReadKeyDetectStateChangeTransaction() {
    try {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      TXManagerImpl txManager =
          (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
      txManager.begin();
      assertThat(region.get(key1)).isEqualTo(value1);
      getBlackboard().signalGate(allowSecondTransactionToProceed);
      getBlackboard().waitForGate(allowReadTransactionCommitToProceed, TIMEOUT_MILLIS,
          MILLISECONDS);
      Throwable thrown = catchThrowable(() -> txManager.commit());
      assertThat(thrown).isInstanceOf(CommitConflictException.class);
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void readTransactionCanBlockWriteTransactionOnReplicateRegion() {
    createReplicateRegionOnServers(regionName);
    createRegionsOnClient(false);

    TransactionId readTXId = client1.invoke(() -> doReadTransaction());
    server1.invoke(() -> setAfterReservationForReadTransaction());
    client1.invokeAsync(() -> commitReadTransaction(readTXId));

    client2.invoke(() -> doPutOnReadKeyTransaction(true));
    client1.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client2.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client1.invoke(() -> verifyClientResults(regionName, key1, value1));
    client2.invoke(() -> verifyClientResults(regionName, key1, value1));
  }

  private void createReplicateRegionOnServers(String name) {
    port1 = server1.invoke(() -> createServerReplicateRegion(name));
    server1.invoke(() -> {
      // make sure key1 is on server1.
      cacheRule.getCache().getRegion(name).put(key1, value1);
    });
    port2 = server2.invoke(() -> createServerReplicateRegion(name));
    server2.invoke(() -> {
      cacheRule.getCache().getRegion(name).put(key2, value2);
      cacheRule.getCache().getRegion(name).put(key3, value3);
    });
  }

  private int createServerReplicateRegion(String name) throws Exception {
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.REPLICATE).create(name);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  @Test
  public void readTransactionDoesNotBlockReadTransactionOnReplicateRegion() {
    createReplicateRegionOnServers(regionName);
    createRegionsOnClient(false);

    TransactionId readTXId = client1.invoke(() -> doReadTransaction());
    server1.invoke(() -> setAfterReservationForReadTransaction());
    client1.invokeAsync(() -> commitReadTransaction(readTXId));

    client2.invoke(() -> doSecondReadTransaction());
    client1.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client2.invoke(() -> verifyClientResults(regionName, key3, newValue3));
    client1.invoke(() -> verifyClientResults(regionName, key1, value1));
    client2.invoke(() -> verifyClientResults(regionName, key1, value1));
  }

  @Test
  public void readConflictsTransactionCanDetectStateChangeOnReplicateRegion() {
    createReplicateRegionOnServers(regionName);
    createRegionsOnClient(false);

    client2.invokeAsync(() -> doPutTransaction());
    client1.invoke(() -> doReadKeyDetectStateChangeTransaction());
    client1.invoke(() -> verifyClientResults(regionName, key1, newValue1));
    client2.invoke(() -> verifyClientResults(regionName, key1, newValue1));
  }

  @Test
  public void transactionsReleaseLocksAfterCommitComplete() {
    createPRRegionOnServers();
    createReplicateRegionOnServers(regionName2);
    createRegionsOnClient(true);

    client2.invoke(() -> addData());

    TransactionId readTXId = client1.invoke(() -> doReadKeysTransaction());
    server1.invoke(() -> setAfterReservationForReadTransaction());
    client1.invokeAsync(() -> commitReadTransaction(readTXId));

    client2.invoke(() -> doPutOnReadKeyTransaction(false));
    client2.invoke(() -> doFailedPutOnReadKeyTransactions());
    client2.invoke(() -> doSuccessfulPutTransactions());
    client2.invoke(() -> {
      getBlackboard().signalGate(allowReadTransactionCommitToProceed);
    });

    client1.invoke(() -> verifyData());
    client2.invoke(() -> verifyData());
  }

  private void addData() {
    Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
    Region<Integer, String> region2 = clientCacheRule.getClientCache().getRegion(regionName2);
    for (int i = 0; i <= 10; i++) {
      region.put(i, "value" + i);
      region2.put(i, "value" + i);
    }
  }

  private TransactionId doReadKeysTransaction() {
    Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
    Region<Integer, String> region2 = clientCacheRule.getClientCache().getRegion(regionName2);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    assertThat(region.get(key1)).isEqualTo(value1);
    assertThat(region2.get(key1)).isEqualTo(value1);
    return txManager.suspend();
  }

  private void doPutOnReadKeyTransaction(boolean doSignal) {
    try {
      getBlackboard().waitForGate(allowSecondTransactionToProceed, TIMEOUT_MILLIS, MILLISECONDS);
      Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
      TXManagerImpl txManager =
          (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
      txManager.begin();
      region.put(key1, newValue1);
      Throwable thrown = catchThrowable(() -> txManager.commit());
      assertThat(thrown).isInstanceOf(CommitConflictException.class);
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (doSignal) {
        getBlackboard().signalGate(allowReadTransactionCommitToProceed);
      }
    }
  }

  private void doFailedPutOnReadKeyTransactions() {
    Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
    Region<Integer, String> region2 = clientCacheRule.getClientCache().getRegion(regionName2);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    for (int i = 0; i <= 10; i++) {
      txManager.begin();
      if (i % 2 != 0) {
        // first key on bucket 1 so transaction hosted on server1
        region.put(i, "failedValue" + i);
        if (i < 9) {
          assertThat(region.get(i + 2)).isEqualTo("value" + (i + 2));
          assertThat(region2.get(i + 1)).isEqualTo("value" + (i + 1));
        }
        region2.put(i, "failedValue" + i);
        // will get conflict during commit on partitioned region key held by the read transaction
        region.put(key1, newValue1);
        region2.put(i + 1, "failedValue" + (i + 1));
      } else {
        // transaction hosted on server2.
        region.put(i, "failedValue" + i);
        if (i < 9) {
          assertThat(region.get(i + 2)).isEqualTo("value" + (i + 2));
          assertThat(region2.get(i + 1)).isEqualTo("value" + (i + 1));
        }
        region2.put(i, "failedValue" + i);
        // will get conflict during commit on replicate region key held by the read transaction
        region2.put(key1, newValue1);
        region2.put(i + 1, "failedValue" + (i + 1));
      }
      Throwable thrown = catchThrowable(() -> txManager.commit());
      assertThat(thrown).isInstanceOf(CommitConflictException.class);
    }
  }

  private void doSuccessfulPutTransactions() {
    Region<Integer, String> region = clientCacheRule.getClientCache().getRegion(regionName);
    Region<Integer, String> region2 = clientCacheRule.getClientCache().getRegion(regionName2);
    TXManagerImpl txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    for (int i = 0; i <= 10; i++) {
      txManager.begin();
      if (i != 1) {
        region.put(i, "newValue" + i);
        region2.put(i, "newValue" + i);
      }
      if (i < 10) {
        assertThat(region2.get(i + 1)).isEqualTo("value" + (i + 1));
      }
      txManager.commit();
    }
  }

  private void verifyData() {
    for (int i = 0; i <= 10; i++) {
      if (i == 1) {
        verifyClientResults(regionName, key1, value1);
        verifyClientResults(regionName2, key1, value1);
      } else {
        verifyClientResults(regionName, i, "newValue" + i);
        verifyClientResults(regionName2, i, "newValue" + i);
      }
    }
  }
}
