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

package org.apache.geode.cache.wan.internal.txgrouping.parallel;

import static org.apache.geode.cache.wan.GatewaySender.GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;

public class TxGroupingParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

  public TxGroupingParallelGatewaySenderQueue(
      final @NotNull AbstractGatewaySender sender,
      final @NotNull Set<Region<?, ?>> userRegions,
      final int idx, final int nDispatcher, final boolean cleanQueues) {
    super(sender, userRegions, idx, nDispatcher, cleanQueues);
  }

  @Override
  protected void postProcessBatch(final @NotNull PartitionedRegion partitionedRegion,
      final @NotNull List<GatewaySenderEventImpl> batch) {
    if (batch.isEmpty()) {
      return;
    }

    final Map<TransactionId, Integer> incompleteTransactionIdsInBatch =
        getIncompleteTransactionsInBatch(batch);
    if (incompleteTransactionIdsInBatch.isEmpty()) {
      return;
    }

    int retries = 0;
    while (true) {
      for (Iterator<Map.Entry<TransactionId, Integer>> iter =
          incompleteTransactionIdsInBatch.entrySet().iterator(); iter.hasNext();) {
        Map.Entry<TransactionId, Integer> pendingTransaction = iter.next();
        TransactionId transactionId = pendingTransaction.getKey();
        int bucketId = pendingTransaction.getValue();
        List<Object> events =
            peekEventsWithTransactionId(partitionedRegion, bucketId, transactionId);
        for (Object object : events) {
          GatewaySenderEventImpl event = (GatewaySenderEventImpl) object;
          batch.add(event);
          peekedEvents.add(event);
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Peeking extra event: {}, bucketId: {}, isLastEventInTransaction: {}, batch size: {}",
                event.getKey(), bucketId, event.isLastEventInTransaction(), batch.size());
          }
          if (event.isLastEventInTransaction()) {
            iter.remove();
          }
        }
      }
      if (incompleteTransactionIdsInBatch.size() == 0 ||
          retries >= sender.getRetriesToGetTransactionEventsFromQueue()) {
        break;
      }
      retries++;
      try {
        Thread.sleep(GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (incompleteTransactionIdsInBatch.size() > 0) {
      logger.warn("Not able to retrieve all events for transactions: {} after {} retries of {}ms",
          incompleteTransactionIdsInBatch, retries, GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS);
      stats.incBatchesWithIncompleteTransactions();
    }
  }

  private Map<TransactionId, Integer> getIncompleteTransactionsInBatch(
      List<GatewaySenderEventImpl> batch) {
    Map<TransactionId, Integer> incompleteTransactionsInBatch = new HashMap<>();
    for (GatewaySenderEventImpl event : batch) {
      if (event.getTransactionId() != null) {
        if (event.isLastEventInTransaction()) {
          incompleteTransactionsInBatch.remove(event.getTransactionId());
        } else {
          incompleteTransactionsInBatch.put(event.getTransactionId(), event.getBucketId());
        }
      }
    }
    return incompleteTransactionsInBatch;
  }

  @Override
  protected void addPreviouslyPeekedEvents(final @NotNull List<GatewaySenderEventImpl> batch,
      final int batchSize) {
    Set<TransactionId> incompleteTransactionsInBatch = new HashSet<>();
    for (int i = 0; i < batchSize || !incompleteTransactionsInBatch.isEmpty(); i++) {
      GatewaySenderEventImpl event = peekedEventsProcessing.remove();
      batch.add(event);
      if (event.getTransactionId() != null) {
        if (event.isLastEventInTransaction()) {
          incompleteTransactionsInBatch.remove(event.getTransactionId());
        } else {
          incompleteTransactionsInBatch.add(event.getTransactionId());
        }
      }
      if (peekedEventsProcessing.isEmpty()) {
        resetLastPeeked = false;
        peekedEventsProcessingInProgress = false;
        break;
      }
    }
  }

}
