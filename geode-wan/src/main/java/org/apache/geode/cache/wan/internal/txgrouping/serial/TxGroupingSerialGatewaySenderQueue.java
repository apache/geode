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

package org.apache.geode.cache.wan.internal.txgrouping.serial;

import static org.apache.geode.cache.wan.GatewaySender.GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;

public class TxGroupingSerialGatewaySenderQueue extends SerialGatewaySenderQueue {

  public TxGroupingSerialGatewaySenderQueue(
      final AbstractGatewaySender abstractSender,
      final String regionName, final CacheListener listener, final boolean cleanQueues) {
    super(abstractSender, regionName, listener, cleanQueues);
  }

  @Override
  protected void incrementEventsNotQueueConflated() {
    // When mustGroupTransactionEvents is true, conflation cannot be enabled.
    // Therefore, if we reach here, it would not be due to a conflated event
    // but rather to an extra peeked event already sent.
  }

  @Override
  protected void postProcessBatch(final List<AsyncEvent<?, ?>> batch, final long lastKey) {
    if (batch.isEmpty()) {
      return;
    }

    Set<TransactionId> incompleteTransactionIdsInBatch = getIncompleteTransactionsInBatch(batch);
    if (incompleteTransactionIdsInBatch.size() == 0) {
      return;
    }

    int retries = 0;
    while (true) {
      for (Iterator<TransactionId> iter = incompleteTransactionIdsInBatch.iterator(); iter
          .hasNext();) {
        TransactionId transactionId = iter.next();
        List<KeyAndEventPair> keyAndEventPairs =
            peekEventsWithTransactionId(transactionId, lastKey);
        if (keyAndEventPairs.size() > 0
            && ((GatewaySenderEventImpl) (keyAndEventPairs.get(keyAndEventPairs.size() - 1)).event)
                .isLastEventInTransaction()) {
          for (KeyAndEventPair object : keyAndEventPairs) {
            GatewaySenderEventImpl event = (GatewaySenderEventImpl) object.event;
            batch.add(event);
            peekedIds.add(object.key);
            extraPeekedIds.add(object.key);
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Peeking extra event: {}, isLastEventInTransaction: {}, batch size: {}",
                  event.getKey(), event.isLastEventInTransaction(), batch.size());
            }
          }
          iter.remove();
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

  private Set<TransactionId> getIncompleteTransactionsInBatch(List<AsyncEvent<?, ?>> batch) {
    Set<TransactionId> incompleteTransactionsInBatch = new HashSet<>();
    for (Object object : batch) {
      if (object instanceof GatewaySenderEventImpl) {
        GatewaySenderEventImpl event = (GatewaySenderEventImpl) object;
        if (event.getTransactionId() != null) {
          if (event.isLastEventInTransaction()) {
            incompleteTransactionsInBatch.remove(event.getTransactionId());
          } else {
            incompleteTransactionsInBatch.add(event.getTransactionId());
          }
        }
      }
    }
    return incompleteTransactionsInBatch;
  }

  private List<KeyAndEventPair> peekEventsWithTransactionId(TransactionId transactionId,
      long lastKey) {
    Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
        x -> transactionId.equals(x.getTransactionId());
    Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
        x -> x.isLastEventInTransaction();

    return getElementsMatching(hasTransactionIdPredicate, isLastEventInTransactionPredicate,
        lastKey);
  }

}
