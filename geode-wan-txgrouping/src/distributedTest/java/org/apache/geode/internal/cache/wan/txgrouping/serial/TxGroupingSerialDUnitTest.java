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
package org.apache.geode.internal.cache.wan.txgrouping.serial;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.txgrouping.TxGroupingBaseDUnitTest;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class TxGroupingSerialDUnitTest extends TxGroupingBaseDUnitTest {
  @Test
  @Parameters({"true", "false"})
  public void testReplicatedSerialPropagationWithVsWithoutGroupTransactionEvents(
      boolean groupTransactionEvents) {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort);
      createReplicatedRegion(REGION_NAME, null);
    });

    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, false,
            groupTransactionEvents, 10, 1);
        createReplicatedRegion(REGION_NAME, newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    // 4 transactions of 3 events each are sent so that the first batch
    // would initially contain the first 3 transactions complete and the first
    // event of the next transaction (10 entries).
    // If --group-transaction-events is configured in the senders, the remaining
    // events of the third transaction are added to the batch which makes
    // that the batch is sent with 12 events.
    // If --group-transaction-events is not configured in the senders, the remaining
    // events of the third transaction are added to the next batch which makes
    // that the 2 batches are sent. One with 10 events and another one
    // with 2 events.
    final int transactions = 4;
    final int eventsPerTransaction = 3;
    final int entries = transactions * eventsPerTransaction;
    int expectedBatchesSent = groupTransactionEvents ? 1 : 2;
    londonServer2VM
        .invoke(() -> doTxPuts(REGION_NAME, eventsPerTransaction, transactions));

    newYorkServerVM.invoke(() -> validateRegionSize(REGION_NAME, entries));

    newYorkServerVM
        .invoke(() -> checkGatewayReceiverStats(expectedBatchesSent, entries, entries, true));

    londonServer1VM.invoke(() -> checkQueueStats(newYorkName, 0, entries, entries, entries));
    londonServer1VM.invoke(() -> checkBatchStats(newYorkName, expectedBatchesSent, false));
    londonServer1VM.invoke(() -> checkConflatedStats(newYorkName));

    // wait until queue is empty
    londonServer2VM.invoke(() -> await()
        .until(() -> getSenderStats(newYorkName, -1).get(0) == 0));

    londonServer2VM.invoke(() -> checkQueueStats(newYorkName, 0, entries, 0, 0));
    londonServer2VM.invoke(() -> checkBatchStats(newYorkName, 0, false));
    londonServer2VM.invoke(() -> checkConflatedStats(newYorkName));
  }

  @Test
  @Parameters({"true", "false"})
  public void testReplicatedSerialPropagationWithGroupTransactionEventsSendsBatchesWithCompleteTransactions_SeveralClients(
      boolean isBatchRedistributed) throws InterruptedException {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort, !isBatchRedistributed);
      createReplicatedRegion(REGION_NAME, null);
    });

    int batchSize = 10;
    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, false,
            true,
            batchSize, 1);
        createReplicatedRegion(REGION_NAME, newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    int clients = 2;
    int eventsPerTransaction = batchSize + 1;
    int transactions = 200;
    int entries = eventsPerTransaction * transactions * clients;
    int entriesPerInvocation = (entries) / clients;

    List<AsyncInvocation<Void>> putAsyncInvocations = new ArrayList<>(clients);
    for (int i = 0; i < clients; i++) {
      final int index = i;
      AsyncInvocation<Void> asyncInvocation =
          londonServer1VM.invokeAsync(() -> doTxPuts(REGION_NAME,
              eventsPerTransaction, transactions, index * entriesPerInvocation));
      putAsyncInvocations.add(asyncInvocation);
    }

    for (AsyncInvocation<Void> invocation : putAsyncInvocations) {
      invocation.await();
    }

    if (isBatchRedistributed) {
      // wait for batches to be redistributed and then start the receiver
      londonServer1VM.invoke(() -> await()
          .until(() -> getSenderStats(newYorkName, -1).get(5) > 0));
      newYorkServerVM.invoke(this::startReceiver);
    }

    newYorkServerVM.invoke(() -> validateRegionSize(REGION_NAME, entries));

    checkQueuesAreEmptyAndOnlyCompleteTransactionsAreReplicated(isBatchRedistributed);
  }

  @Test
  @Parameters({"true", "false"})
  public void testReplicatedSerialPropagationWithVsWithoutGroupTransactionEventsWithBatchRedistribution(
      boolean groupTransactionEvents) {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort, false);
      createReplicatedRegion(REGION_NAME, null);
    });

    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, false,
            groupTransactionEvents, 10, 1);
        createReplicatedRegion(REGION_NAME, newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    // 8 transactions of 3 events each are sent.
    // - With group-transaction-events
    // The first batch would initially contain the first 3 transactions complete
    // and the first event of the next transaction (10 entries).
    // As --group-transaction-events is configured in the senders, the remaining
    // events of the third transaction are added to the batch which makes
    // that the first batch is sent with 12 events. The same happens with the
    // second batch which will contain 12 events too.
    // - Without group-transaction-events 3 batches are sent, 2 with 10 events
    // and one with 4.
    final int transactions = 8;
    final int eventsPerTransaction = 3;
    final int entries = transactions * eventsPerTransaction;
    int expectedBatchesSent = groupTransactionEvents ? 2 : 3;

    londonServer2VM
        .invoke(() -> doTxPuts(REGION_NAME, eventsPerTransaction, transactions));

    // wait for batches to be redistributed and then start the receiver
    londonServer1VM.invoke(() -> await()
        .untilAsserted(() -> assertThat(getSenderStats(newYorkName, -1).get(5)).isGreaterThan(0)));

    newYorkServerVM.invoke(this::startReceiver);

    newYorkServerVM.invoke(() -> validateRegionSize(REGION_NAME, entries));


    newYorkServerVM
        .invoke(() -> checkGatewayReceiverStats(expectedBatchesSent, entries, entries, true));

    londonServer1VM.invoke(() -> checkQueueStats(newYorkName, 0, entries, entries, entries));
    londonServer1VM.invoke(() -> checkBatchStats(newYorkName, expectedBatchesSent, true));

    // wait until queue is empty
    londonServer2VM.invoke(() -> getSenderStats(newYorkName, 0));

    londonServer2VM.invoke(() -> checkQueueStats(newYorkName, 0, entries, 0, 0));
    londonServer2VM.invoke(() -> checkBatchStats(newYorkName, 0, false));
  }

  @Test
  public void testReplicatedSerialPropagationHAWithGroupTransactionEvents() throws Exception {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort);
      createReplicatedRegion(REGION_NAME, null);
    });

    int batchSize = 9;
    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, false,
            true, batchSize, 1);
        createReplicatedRegion(REGION_NAME, newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    int putsPerTransaction = 2;
    int transactions = 5000;
    AsyncInvocation<Void> putsInvocation1 =
        londonServer3VM.invokeAsync(
            () -> doTxPutsWithRetryIfError(REGION_NAME, putsPerTransaction, transactions, 0));
    AsyncInvocation<Void> putsInvocation2 =
        londonServer4VM.invokeAsync(
            () -> doTxPutsWithRetryIfError(REGION_NAME, putsPerTransaction, transactions,
                putsPerTransaction * transactions));

    newYorkServerVM.invoke(() -> await()
        .untilAsserted(() -> assertThat(getRegionSize(REGION_NAME)).isGreaterThan(40)));

    AsyncInvocation<Boolean> killServerAsyncInvocation =
        londonServer1VM.invokeAsync(() -> killPrimarySender(newYorkName));
    Boolean isKilled = killServerAsyncInvocation.get();
    if (!isKilled) {
      AsyncInvocation<Boolean> killServerAsyncInvocation2 =
          londonServer2VM.invokeAsync(() -> killPrimarySender(newYorkName));
      killServerAsyncInvocation2.await();
    }
    putsInvocation1.await();
    putsInvocation2.await();
    killServerAsyncInvocation.await();

    int entries = 2 * putsPerTransaction * transactions;
    londonServer2VM.invoke(() -> validateRegionSize(REGION_NAME, entries));
    newYorkServerVM.invoke(() -> validateRegionSize(REGION_NAME, entries));

    // batchesReceived is equal to numberOfEntries/(batchSize+1)
    // As transactions are 2 events long, for each batch it will always be necessary to
    // add one more entry to the 9 events batch in order to have complete transactions in the batch.
    int batchesReceived = entries / (batchSize + 1);
    newYorkServerVM.invoke(() -> checkGatewayReceiverStatsHA(batchesReceived, entries, entries));

    londonServer2VM.invoke(() -> checkStats_Failover(newYorkName, entries));
  }

  private void checkQueuesAreEmptyAndOnlyCompleteTransactionsAreReplicated(
      boolean isBatchesRedistributed) {
    // Wait for sender queues to be empty
    List<List<Integer>> londonServersStats = new ArrayList(londonServersVM.length);
    for (VM londonServer : londonServersVM) {
      londonServersStats.add(londonServer.invoke(() -> getSenderStats(newYorkName, 0)));
    }

    int queueSize = londonServersStats.stream().map(x -> x.get(0)).reduce(0, Integer::sum);
    assertThat(queueSize).isEqualTo(0);

    // batches redistributed:
    int batchesRedistributed =
        londonServersStats.stream().map(x -> x.get(5)).reduce(0, Integer::sum);
    if (isBatchesRedistributed) {
      assertThat(batchesRedistributed).isGreaterThan(0);
    } else {
      assertThat(batchesRedistributed).isEqualTo(0);
    }
  }

  private void createReplicatedRegion(String regionName, String senderId) {
    RegionFactory<Object, Object> fact =
        cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    if (senderId != null) {
      fact.addGatewaySenderId(senderId);
    }
    fact.create(regionName);
  }

  private void checkQueueStats(String senderId, final int queueSize, final int eventsReceived,
      final int eventsQueued, final int eventsDistributed) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertThat(statistics.getEventQueueSize()).isEqualTo(queueSize);
    assertThat(statistics.getEventsReceived()).isEqualTo(eventsReceived);
    assertThat(statistics.getEventsQueued()).isEqualTo(eventsQueued);
    assertThat(statistics.getEventsDistributed()).isGreaterThanOrEqualTo(eventsDistributed);
  }

  private void checkBatchStats(String senderId, final int batches,
      final boolean batchesRedistributed) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertThat(statistics.getBatchesDistributed()).isEqualTo(batches);

    if (batchesRedistributed) {
      assertThat(statistics.getBatchesRedistributed()).isGreaterThan(0);
    } else {
      assertThat(statistics.getBatchesRedistributed()).isEqualTo(0);
    }
  }

  private void checkConflatedStats(String senderId) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertThat(statistics.getEventsNotQueuedConflated()).isEqualTo(0);
  }

  private void checkStats_Failover(String senderId, final int eventsReceived) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertThat(statistics.getEventsReceived()).isEqualTo(eventsReceived);
    assertThat((statistics.getEventsQueued() + statistics.getUnprocessedTokensAddedByPrimary()
        + statistics.getUnprocessedEventsRemovedByPrimary())).isEqualTo(eventsReceived);
  }

  private boolean killPrimarySender(String senderId) {
    try (IgnoredException ignoredException1 =
        IgnoredException.addIgnoredException("Could not connect");
        IgnoredException ignoredException2 =
            IgnoredException.addIgnoredException(CacheClosedException.class.getName());
        IgnoredException ignoredException3 =
            IgnoredException.addIgnoredException(ForceReattemptException.class.getName())) {
      AbstractGatewaySender sender = (AbstractGatewaySender) getGatewaySender(senderId);
      if (sender.isPrimary()) {
        cacheRule.getCache().close();
        return true;
      }
      return false;
    }
  }
}
