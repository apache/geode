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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;

public class TxGroupingSerialGatewaySenderQueue extends SerialGatewaySenderQueue {

  /**
   * Contains the set of peekedIds that were peeked to complete a transaction
   * inside a batch when groupTransactionEvents is set.
   */
  protected final Set<Long> extraPeekedIds = ConcurrentHashMap.newKeySet();

  /**
   * Contains the set of peekedIds that were peeked to complete a transaction
   * inside a batch when groupTransactionEvents is set and whose event has been
   * removed from the queue because an ack has been received from the receiver.
   * Elements from this set are deleted when the event with the previous id
   * is removed.
   */
  private final Set<Long> extraPeekedIdsRemovedButPreviousIdNotRemoved =
      ConcurrentHashMap.newKeySet();

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

  /**
   * This method returns a list of objects that fulfill the matchingPredicate
   * If a matching object also fulfills the endPredicate then the method
   * stops looking for more matching objects.
   */
  protected List<KeyAndEventPair> getElementsMatching(Predicate<GatewaySenderEventImpl> condition,
      Predicate<GatewaySenderEventImpl> stopCondition,
      long lastKey) {
    GatewaySenderEventImpl event;
    List<KeyAndEventPair> elementsMatching = new ArrayList<>();

    long currentKey = lastKey;

    while ((currentKey = inc(currentKey)) != getTailKey()) {
      if (extraPeekedIds.contains(currentKey)) {
        continue;
      }
      event = (GatewaySenderEventImpl) optimalGet(currentKey);
      if (event == null) {
        continue;
      }

      if (condition.test(event)) {
        elementsMatching.add(new KeyAndEventPair(currentKey, event));

        if (stopCondition.test(event)) {
          break;
        }
      }
    }

    return elementsMatching;
  }

  @Override
  public synchronized void remove() throws CacheException {
    if (peekedIds.isEmpty()) {
      return;
    }
    boolean wasEmpty = lastDispatchedKey == lastDestroyedKey;
    Long key = peekedIds.remove();
    boolean isExtraPeekedId = extraPeekedIds.contains(key);
    if (!isExtraPeekedId) {
      updateHeadKey(key);
      lastDispatchedKey = key;
    } else {
      extraPeekedIdsRemovedButPreviousIdNotRemoved.add(key);
    }
    removeIndex(key);
    // Remove the entry at that key with a callback arg signifying it is
    // a WAN queue so that AbstractRegionEntry.destroy can get the value
    // even if it has been evicted to disk. In the normal case, the
    // AbstractRegionEntry.destroy only gets the value in the VM.
    try {
      this.region.localDestroy(key, WAN_QUEUE_TOKEN);
      this.stats.decQueueSize();
    } catch (EntryNotFoundException ok) {
      // this is acceptable because the conflation can remove entries
      // out from underneath us.
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Did not destroy entry at {} it was not there. It should have been removed by conflation.",
            this, key);
      }
    }

    // For those extraPeekedIds removed that are consecutive to lastDispatchedKey:
    // - Update lastDispatchedKey with them so that they are removed
    // by the batch removal thread.
    // - Update the head key with them.
    // - Remove them from extraPeekedIds.
    long tmpKey = lastDispatchedKey;
    while (extraPeekedIdsRemovedButPreviousIdNotRemoved.contains(tmpKey = inc(tmpKey))) {
      extraPeekedIdsRemovedButPreviousIdNotRemoved.remove(tmpKey);
      extraPeekedIds.remove(tmpKey);
      updateHeadKey(tmpKey);
      lastDispatchedKey = tmpKey;
    }

    if (wasEmpty) {
      synchronized (this) {
        notifyAll();
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Destroyed entry at key {} setting the lastDispatched Key to {}. The last destroyed entry was {}",
          this, key, this.lastDispatchedKey, this.lastDestroyedKey);
    }
  }

  public void resetLastPeeked() {
    super.resetLastPeeked();
    extraPeekedIds.clear();
  }

  @Override
  protected boolean skipPeekedKey(Long key) {
    return extraPeekedIds.contains(key);
  }

  @VisibleForTesting
  Set<Long> getExtraPeekedIds() {
    return Collections.unmodifiableSet(extraPeekedIds);
  }
}
