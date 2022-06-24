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
package org.apache.geode.internal.cache.wan.txgrouping.parallel;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.parallel.ParallelGatewaySenderImpl;
import org.apache.geode.cache.wan.internal.txgrouping.parallel.TxGroupingParallelGatewaySenderImpl;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.txgrouping.TxGroupingBaseDUnitTest;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class TxGroupingParallelDUnitTest extends TxGroupingBaseDUnitTest {
  @Test
  @Parameters({TxGroupingParallelGatewaySenderImpl.TYPE, ParallelGatewaySenderImpl.TYPE})
  public void testPRParallelPropagationWithVsWithoutGroupTransactionEvents(
      String type) {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort);
      createCustomerOrderShipmentPartitionedRegion(null);
    });

    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, type,
            10);
        createCustomerOrderShipmentPartitionedRegion(newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    // 3 transactions of 4 events each are sent so that the batch would
    // initially contain the first 2 transactions complete and the first
    // 2 events of the last transaction (10 entries).
    // If --group-transaction-events is configured in the senders, the remaining
    // 2 events of the last transaction are added to the batch which makes
    // that only one batch of 12 events is sent.
    // If --group-transaction-events is not configured in the senders, the
    // remaining 2 events of the last transaction are added to the second batch
    // which makes that 2 batches will be sent, one with 10 events and
    // one with 2.
    int transactions = 3;
    int eventsPerTransaction = 4;
    londonServer1VM.invoke(
        () -> doOrderAndShipmentPutsInsideTransactions(0, eventsPerTransaction, transactions));

    int entries = transactions * eventsPerTransaction;

    londonServer1VM.invoke(() -> validateRegionSize(orderRegionName, transactions));
    londonServer1VM.invoke(() -> validateRegionSize(shipmentRegionName, transactions * 3));

    List<Integer> senderStatsLondonServers = getSenderStats(newYorkName, 0, londonServersVM);

    int expectedBatchesSent = type.equals(TxGroupingParallelGatewaySenderImpl.TYPE) ? 1 : 2;
    // queue size:
    assertThat(senderStatsLondonServers.get(0)).isEqualTo(0);
    // eventsReceived:
    assertThat(senderStatsLondonServers.get(1)).isEqualTo(entries);
    // events queued:
    assertThat(senderStatsLondonServers.get(2)).isEqualTo(entries);
    // events distributed:
    assertThat(senderStatsLondonServers.get(3)).isEqualTo(entries);
    // batches distributed:
    assertThat(senderStatsLondonServers.get(4)).isEqualTo(expectedBatchesSent);
    // batches redistributed:
    assertThat(senderStatsLondonServers.get(5)).isEqualTo(0);
    // events not queued conflated:
    assertThat(senderStatsLondonServers.get(7)).isEqualTo(0);
    // batches with incomplete transactions:
    assertThat(senderStatsLondonServers.get(13)).isEqualTo(0);
  }

  @Test
  @Parameters({"true", "false"})
  public void testPRParallelPropagationWithGroupTransactionEventsSendsBatchesWithCompleteTransactions_SeveralClients(
      boolean isBatchesRedistributed) throws InterruptedException {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort, !isBatchesRedistributed);
      createCustomerOrderShipmentPartitionedRegion(null);
    });

    int batchSize = 10;
    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName,
            TxGroupingParallelGatewaySenderImpl.TYPE,
            batchSize);
        createCustomerOrderShipmentPartitionedRegion(newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    // batchSize is 10. Each transaction will contain 1 order + 3 shipments = 4 events.
    // As a result, all batches will contain extra events to complete the
    // transactions it will deliver.
    int shipmentsPerTransaction = 3;
    int eventsPerTransaction = shipmentsPerTransaction + 1;
    int transactions = 300;

    int clients = 4;
    for (int i = 0; i < clients; i++) {
      final Map<Object, Object> custKeyValue = new HashMap<>();
      custKeyValue.put(new CustId(i), new Customer());
      londonServer1VM.invoke(() -> putGivenKeyValues(customerRegionName, custKeyValue));
    }

    List<AsyncInvocation<?>> asyncInvocations = new ArrayList<>(clients);

    for (int i = 0; i < clients; i++) {
      final int intCustId = i;
      AsyncInvocation<?> asyncInvocation =
          londonServer1VM.invokeAsync(() -> doOrderAndShipmentPutsInsideTransactions(
              intCustId, eventsPerTransaction, transactions));
      asyncInvocations.add(asyncInvocation);
    }

    for (AsyncInvocation<?> asyncInvocation : asyncInvocations) {
      asyncInvocation.await();
    }

    londonServer1VM.invoke(() -> validateRegionSize(customerRegionName, clients));
    londonServer1VM.invoke(() -> validateRegionSize(orderRegionName, transactions * clients));
    londonServer1VM.invoke(() -> validateRegionSize(shipmentRegionName,
        transactions * shipmentsPerTransaction * clients));

    if (isBatchesRedistributed) {
      // wait for batches to be redistributed and then start the receiver
      londonServer1VM.invoke(() -> await()
          .until(() -> getSenderStats(newYorkName, -1).get(5) > 0));
      newYorkServerVM.invoke("start New York receiver", this::startReceiver);
    }

    // Check that all entries have been written in the receiver
    newYorkServerVM.invoke(
        () -> validateRegionSize(customerRegionName, clients));
    newYorkServerVM.invoke(
        () -> validateRegionSize(orderRegionName, transactions * clients));
    newYorkServerVM.invoke(
        () -> validateRegionSize(shipmentRegionName,
            shipmentsPerTransaction * transactions * clients));

    checkQueuesAreEmptyAndOnlyCompleteTransactionsAreReplicated(newYorkName,
        isBatchesRedistributed);
  }

  @Test
  public void testPRParallelPropagationWithGroupTransactionEventsWithIncompleteTransactionsWhenTransactionEntriesOnNotColocatedBuckets() {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort);
      createPartitionedRegion(REGION_NAME, null);
    });

    int dispatcherThreads = 2;
    londonServer1VM.invoke("create London server " + londonServer1VM.getId(), () -> {
      startServerWithSender(londonServer1VM.getId(), londonLocatorPort, newYorkId, newYorkName,
          TxGroupingParallelGatewaySenderImpl.TYPE, 10, dispatcherThreads);
      createPartitionedRegion(REGION_NAME, newYorkName);
      GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
      await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
    });

    // Adding events in transactions
    // Transactions will contain objects assigned to different buckets but given that there is only
    // one server, there will be no TransactionDataNotCollocatedException.
    // With this and by using more than one dispatcher thread, we will provoke that
    // it will be impossible for the batches to have complete transactions as some
    // events for a transaction will be handled by one dispatcher thread and some other events by
    // another thread.
    int entriesPerTransaction = 3;
    int transactions = 10;
    int entries = transactions * entriesPerTransaction;

    londonServer1VM
        .invoke(() -> doTxPuts(REGION_NAME, 3, 10));

    londonServer1VM.invoke(() -> validateRegionSize(REGION_NAME, entries));

    ArrayList<Integer> senderStatsLondonServer1 =
        (ArrayList<Integer>) londonServer1VM.invoke(() -> getSenderStats(newYorkName, 0));

    // The number of batches will be 4 because each
    // dispatcher thread (there are 2) will send half the number of entries,
    // each on 2 batches.
    int batches = 4;
    // queue size:
    assertThat(senderStatsLondonServer1.get(0)).isEqualTo(0);
    // eventsReceived:
    assertThat(senderStatsLondonServer1.get(1)).isEqualTo(entries);
    // events queued:
    assertThat(senderStatsLondonServer1.get(2)).isEqualTo(entries);
    // events distributed:
    assertThat(senderStatsLondonServer1.get(3)).isEqualTo(entries);
    // batches distributed:
    assertThat(senderStatsLondonServer1.get(4)).isEqualTo(batches);
    // batches redistributed:
    assertThat(senderStatsLondonServer1.get(5)).isEqualTo(0);
    // events not queued conflated:
    assertThat(senderStatsLondonServer1.get(7)).isEqualTo(0);
    // batches with incomplete transactions
    assertThat(senderStatsLondonServer1.get(13)).isEqualTo(batches);

    newYorkServerVM.invoke(() -> checkGatewayReceiverStats(batches, entries, entries));
  }

  @Test
  @Parameters({TxGroupingParallelGatewaySenderImpl.TYPE, ParallelGatewaySenderImpl.TYPE})
  public void testPRParallelPropagationWithVsWithoutGroupTransactionEventsWithBatchRedistribution(
      String type) {
    londonServer1VM.invoke("create London server " + londonServer1VM.getId(), () -> {
      startServerWithSender(londonServer1VM.getId(), londonLocatorPort, newYorkId, newYorkName,
          type, 10);
      createCustomerOrderShipmentPartitionedRegion(newYorkName);
      GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
      await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
    });

    newYorkServerVM.invoke("create New York server with receiver stopped", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort, false);
      createCustomerOrderShipmentPartitionedRegion(null);
    });

    // 6 transactions of 4 events each are sent with batch size = 10
    // - With group transaction events:
    // The first batch would initially contain the first 2 transactions complete and the first
    // 2 events of the next transaction (10 entries).
    // As --group-transaction-events is configured in the senders, the remaining
    // 2 events of the second transaction are added to the batch which makes
    // the first batch to be sent with 12 events. The same happens with the
    // second batch which will contain 12 events too.
    // - Without group-transaction-events 3 batches will be sent. 2
    // with 10 events and one with 4.
    int transactions = 6;
    int eventsPerTransaction = 4;
    int expectedBatchesSent;
    if (type.endsWith(TxGroupingParallelGatewaySenderImpl.TYPE)) {
      expectedBatchesSent = 2;
    } else {
      expectedBatchesSent = 3;
    }
    londonServer1VM.invoke(
        () -> doOrderAndShipmentPutsInsideTransactions(0, eventsPerTransaction, transactions));

    int entries = transactions * eventsPerTransaction;

    londonServer1VM.invoke(() -> validateRegionSize(orderRegionName, transactions));
    londonServer1VM.invoke(() -> validateRegionSize(shipmentRegionName, transactions * 3));

    // wait for batches to be redistributed and then start the receiver
    londonServer1VM.invoke(() -> await()
        .until(() -> getSenderStats(newYorkName, -1).get(5) > 0));

    newYorkServerVM.invoke("Start New York receiver", this::startReceiver);

    ArrayList<Integer> senderStatsLondonServer1 =
        (ArrayList<Integer>) londonServer1VM.invoke(() -> getSenderStats(newYorkName, 0));

    // queue size:
    assertThat(senderStatsLondonServer1.get(0)).isEqualTo(0);
    // events received:
    assertThat(senderStatsLondonServer1.get(1)).isEqualTo(entries);
    // events queued:
    assertThat(senderStatsLondonServer1.get(2)).isEqualTo(entries);
    // events distributed:
    assertThat(senderStatsLondonServer1.get(3)).isEqualTo(entries);
    // batches distributed:
    assertThat(senderStatsLondonServer1.get(4)).isEqualTo(expectedBatchesSent);
    // batches redistributed:
    assertThat(senderStatsLondonServer1.get(5)).isGreaterThan(0);
    // events not queued conflated:
    assertThat(senderStatsLondonServer1.get(7)).isEqualTo(0);
  }

  @Test
  public void testParallelPropagationHAWithGroupTransactionEvents() throws Exception {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort);
      createPartitionedRegion(orderRegionName, null);
      createPartitionedRegion(shipmentRegionName, null);
    });

    int batchSize = 11;
    int redundantCopies = 3;
    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName,
            TxGroupingParallelGatewaySenderImpl.TYPE,
            batchSize, redundantCopies);
        createCustomerOrderShipmentPartitionedRegion(newYorkName, redundantCopies);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    // putsPerTransaction = 1 order + 3 shipments
    int putsPerTransaction = 4;
    int transactions = 1000;
    AsyncInvocation<Void> asyncPutInvocation =
        londonServer2VM.invokeAsync(
            () -> doOrderAndShipmentPutsInsideTransactions(0, putsPerTransaction, transactions,
                true));

    newYorkServerVM.invoke(() -> await()
        .untilAsserted(() -> assertThat(getRegionSize(shipmentRegionName)).isGreaterThan(40)));
    AsyncInvocation<Void> killServerInvocation =
        londonServer1VM.invokeAsync(() -> cacheRule.getCache().close());

    asyncPutInvocation.await();
    killServerInvocation.await();

    int entries = transactions * putsPerTransaction;
    newYorkServerVM
        .invoke(() -> validateRegionSize(shipmentRegionName, transactions * 3));
    newYorkServerVM
        .invoke(() -> validateRegionSize(orderRegionName, transactions));

    List<Integer> londonServerStats =
        getSenderStats(newYorkName, 0, (VM[]) ArrayUtils.remove(londonServersVM, 0));

    // queue size
    assertThat(londonServerStats.get(0)).isEqualTo(0);

    // eventsReceived
    // We may see 4 retried events (as transactions are made of 4 events) on all members due to
    // the kill
    assertThat(londonServerStats.get(1))
        .isLessThanOrEqualTo((entries + putsPerTransaction) * redundantCopies);
    assertThat(londonServerStats.get(1)).isGreaterThanOrEqualTo(entries * redundantCopies);

    // queuedEvents
    assertThat(londonServerStats.get(2))
        .isLessThanOrEqualTo((entries + putsPerTransaction) * redundantCopies);
    assertThat(londonServerStats.get(2)).isGreaterThanOrEqualTo(entries * redundantCopies);

    // batches redistributed
    assertThat(londonServerStats.get(5)).isEqualTo(0);

    // batchesReceived is equal to numberOfEntries/(batchSize+1)
    // As transactions are 4 events long, for each batch it will always be necessary to
    // add one more entry to the 11 events batch in order to have complete transactions in the
    // batch.
    int batchesReceived = (entries) / (batchSize + 1);
    newYorkServerVM.invoke(() -> checkGatewayReceiverStatsHA(batchesReceived, entries, entries));
  }

  private void checkQueuesAreEmptyAndOnlyCompleteTransactionsAreReplicated(String senderId,
      boolean isBatchesRedistributed) {
    List<Integer> senderStatsLondonServers = getSenderStats(senderId, 0, londonServersVM);

    // queue size:
    assertThat(senderStatsLondonServers.get(0)).isEqualTo(0);
    // batches redistributed:
    int batchesRedistributed = senderStatsLondonServers.get(5);
    if (isBatchesRedistributed) {
      assertThat(batchesRedistributed).isGreaterThan(0);
    } else {
      assertThat(batchesRedistributed).isEqualTo(0);
    }
    // batches with incomplete transactions
    assertThat(senderStatsLondonServers.get(13)).isEqualTo(0);

    for (VM londonServer : londonServersVM) {
      londonServer.invoke(() -> validateGatewaySenderQueueAllBucketsDrained(senderId));
    }
  }

  protected void validateGatewaySenderQueueAllBucketsDrained(final String senderId) {
    try (IgnoredException ignoredE1 = addIgnoredException(RegionDestroyedException.class);
        IgnoredException ignoredE2 = addIgnoredException(ForceReattemptException.class)) {
      GatewaySender sender = getGatewaySender(senderId);
      final AbstractGatewaySender abstractSender = (AbstractGatewaySender) sender;
      await().untilAsserted(() -> assertThat(abstractSender.getEventQueueSize()).isEqualTo(0));
      await().untilAsserted(
          () -> assertThat(abstractSender.getSecondaryEventQueueSize()).isEqualTo(0));
    }
  }

  public void createPartitionedRegion(String regionName, String senderId) {
    createPartitionedRegion(regionName, senderId, 0);
  }

  public void createPartitionedRegion(String regionName, String senderId, int redundantCopies) {
    RegionFactory<Object, Object> fact =
        cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    if (senderId != null) {
      fact.addGatewaySenderId(senderId);
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setRedundantCopies(redundantCopies);
    pfact.setTotalNumBuckets(10);
    fact.setPartitionAttributes(pfact.create());
    fact.create(regionName);
  }

  protected List<Integer> getSenderStats(String senderId, int expectedQueueSize,
      VM[] servers) {
    List<Integer> stats = null;
    for (VM server : servers) {
      List<Integer> serverStats =
          server.invoke(() -> getSenderStats(senderId, expectedQueueSize));
      if (stats == null) {
        stats = serverStats;
      } else {
        for (int i = 0; i < stats.size(); i++) {
          stats.set(i, stats.get(i) + serverStats.get(i));
        }
      }
    }
    return stats;
  }
}
