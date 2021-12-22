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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_BATCH_SIZE;
import static org.apache.geode.cache.wan.GatewaySender.GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.event.NonDistributedEventTracker;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.offheap.OffHeapClearRequired;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.beans.AsyncEventQueueMBean;
import org.apache.geode.management.internal.beans.GatewaySenderMBean;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * @since GemFire 7.0
 */
public class SerialGatewaySenderQueue implements RegionQueue {

  private static final Logger logger = LogService.getLogger();

  /**
   * The key into the <code>Region</code> used when taking entries from the queue. This value is
   * either set when the queue is instantiated or read from the <code>Region</code> in the case
   * where this queue takes over where a previous one left off.
   */
  private long headKey = -1;

  /**
   * The key into the <code>Region</code> used when putting entries onto the queue. This value is
   * either set when the queue is instantiated or read from the <code>Region</code> in the case
   * where this queue takes over where a previous one left off.
   */
  private final AtomicLong tailKey = new AtomicLong();

  /**
   * Last key peeked from the queue excluding the keys peeked
   * to complete transactions when group-transaction-events is enabled.
   */
  private final AtomicLong lastPeekedId = new AtomicLong(-1);

  private final Deque<Long> peekedIds = new LinkedBlockingDeque<>();

  /**
   * Contains the set of peekedIds that were peeked to complete a transaction
   * inside a batch when groupTransactionEvents is set.
   */
  private final Set<Long> extraPeekedIds = ConcurrentHashMap.newKeySet();

  /**
   * Contains the set of peekedIds that were peeked to complete a transaction
   * inside a batch when groupTransactionEvents is set and whose event has been
   * removed from the queue because an ack has been received from the receiver.
   * Elements from this set are deleted when the event with the previous id
   * is removed.
   */
  private final Set<Long> extraPeekedIdsRemovedButPreviousIdNotRemoved =
      ConcurrentHashMap.newKeySet();

  /**
   * The name of the <code>Region</code> backing this queue
   */
  private final String regionName;

  /**
   * The <code>Region</code> backing this queue
   */
  private Region<Long, AsyncEvent> region;

  /**
   * The name of the <code>DiskStore</code> to overflow this queue
   */
  private final String diskStoreName;

  /**
   * The maximum number of entries in a batch.
   */
  private final int batchSize;

  /**
   * The maximum amount of memory (MB) to allow in the queue before overflowing entries to disk
   */
  private final int maximumQueueMemory;

  /**
   * Whether conflation is enabled for this queue.
   */
  private final boolean enableConflation;

  /**
   * Whether persistence is enabled for this queue.
   */
  private final boolean enablePersistence;

  private final boolean cleanQueues;

  /**
   * Whether write to disk is synchronous.
   */
  private final boolean isDiskSynchronous;

  /**
   * The <code>Map</code> mapping the regionName->key to the queue key. This index allows fast
   * updating of entries in the queue for conflation.
   */
  private final Map<String, Map<Object, Long>> indexes;

  private final GatewaySenderStats stats;

  /**
   * The maximum allowed key before the keys are rolled over
   */
  private static final long MAXIMUM_KEY = Long.MAX_VALUE;

  /**
   * Whether the <code>Gateway</code> queue should be no-ack instead of ack.
   */
  private static final boolean NO_ACK =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "gateway-queue-no-ack");

  private volatile long lastDispatchedKey = -1;

  private volatile long lastDestroyedKey = -1;

  public static final int DEFAULT_MESSAGE_SYNC_INTERVAL = 1;

  @Immutable
  private static final int messageSyncInterval = DEFAULT_MESSAGE_SYNC_INTERVAL;

  private BatchRemovalThread removalThread = null;

  private AbstractGatewaySender sender = null;

  private final MetaRegionFactory metaRegionFactory;

  public SerialGatewaySenderQueue(AbstractGatewaySender abstractSender, String regionName,
      CacheListener listener, boolean cleanQueues) {
    this(abstractSender, regionName, listener, cleanQueues, new MetaRegionFactory());
  }

  public SerialGatewaySenderQueue(AbstractGatewaySender abstractSender, String regionName,
      CacheListener listener, boolean cleanQueues, MetaRegionFactory metaRegionFactory) {
    // The queue starts out with headKey and tailKey equal to -1 to force
    // them to be initialized from the region.
    this.regionName = regionName;
    this.cleanQueues = cleanQueues;
    this.metaRegionFactory = metaRegionFactory;
    headKey = -1;
    tailKey.set(-1);
    indexes = new HashMap<>();
    enableConflation = abstractSender.isBatchConflationEnabled();
    diskStoreName = abstractSender.getDiskStoreName();
    batchSize = abstractSender.getBatchSize();
    enablePersistence = abstractSender.isPersistenceEnabled();
    if (enablePersistence) {
      isDiskSynchronous = abstractSender.isDiskSynchronous();
    } else {
      isDiskSynchronous = false;
    }
    maximumQueueMemory = abstractSender.getMaximumMemeoryPerDispatcherQueue();
    stats = abstractSender.getStatistics();
    initializeRegion(abstractSender, listener);
    // Increment queue size. Fix for bug 51988.
    stats.incQueueSize(region.size());
    removalThread = new BatchRemovalThread(abstractSender.getCache());
    removalThread.start();
    sender = abstractSender;
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Contains {} elements", this, size());
    }
  }

  @Override
  public Region<Long, AsyncEvent> getRegion() {
    return region;
  }

  public void destroy() {
    getRegion().localDestroyRegion();
  }

  @Override
  public synchronized boolean put(Object event) throws CacheException {
    GatewaySenderEventImpl eventImpl = (GatewaySenderEventImpl) event;
    final Region r = eventImpl.getRegion();
    final boolean isPDXRegion =
        (r instanceof DistributedRegion && r.getName().equals(PeerTypeRegistration.REGION_NAME));
    final boolean isWbcl =
        regionName.startsWith(AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX);
    if (!(isPDXRegion && isWbcl)) {
      putAndGetKey(event);
      return true;
    }
    return false;
  }

  private long putAndGetKey(Object object) throws CacheException {
    // Get the tail key
    Long key = Long.valueOf(getTailKey());
    // Put the object into the region at that key
    region.put(key, (AsyncEvent) object);

    // Increment the tail key
    // It is important that we increment the tail
    // key after putting in the region, this is the
    // signal that a new object is available.
    incrementTailKey();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Inserted {} -> {}", this, key, object);
    }
    if (object instanceof Conflatable) {
      removeOldEntry((Conflatable) object, key);
    }
    return key.longValue();
  }


  @Override
  public AsyncEvent take() throws CacheException {
    // Unsupported since we have no callers.
    // If we do want to support it then each caller needs
    // to call freeOffHeapResources and the returned GatewaySenderEventImpl
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AsyncEvent> take(int batchSize) throws CacheException {
    // This method has no callers.
    // If we do want to support it then the callers
    // need to call freeOffHeapResources on each returned GatewaySenderEventImpl
    throw new UnsupportedOperationException();
  }

  /**
   * This method removes the last entry. However, it will only let the user remove entries that they
   * have peeked. If the entry was not peeked, this method will silently return.
   */
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
      region.localDestroy(key, WAN_QUEUE_TOKEN);
      stats.decQueueSize();
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
          this, key, lastDispatchedKey, lastDestroyedKey);
    }
  }

  /**
   * This method removes batchSize entries from the queue. It will only remove entries that were
   * previously peeked.
   *
   * @param size the number of entries to remove
   */
  @Override
  public void remove(int size) throws CacheException {
    for (int i = 0; i < size; i++) {
      remove();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Removed a batch of {} entries", this, size);
    }
  }

  @Override
  public Object peek() throws CacheException {
    KeyAndEventPair object = peekAhead();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Peeked {} -> {}", this, peekedIds, object);
    }

    return object.event;
    // OFFHEAP returned object only used to see if queue is empty
    // so no need to worry about off-heap refCount.
  }

  @Override
  public List<AsyncEvent<?, ?>> peek(int size) throws CacheException {
    return peek(size, -1);
  }

  @Override
  public List<AsyncEvent<?, ?>> peek(int size, int timeToWait) throws CacheException {
    final boolean isTraceEnabled = logger.isTraceEnabled();

    long start = System.currentTimeMillis();
    long end = start + timeToWait;
    if (isTraceEnabled) {
      logger.trace("{}: Peek start time={} end time={} time to wait={}", this, start, end,
          timeToWait);
    }

    List<AsyncEvent<?, ?>> batch =
        new ArrayList<>(size == BATCH_BASED_ON_TIME_ONLY ? DEFAULT_BATCH_SIZE : size);
    long lastKey = -1;
    while (size == BATCH_BASED_ON_TIME_ONLY || batch.size() < size) {
      KeyAndEventPair pair = peekAhead();
      // Conflate here
      if (pair != null) {
        AsyncEvent<?, ?> object = pair.event;
        lastKey = pair.key;
        batch.add(object);
      } else {
        // If time to wait is -1 (don't wait) or time interval has elapsed
        long currentTime = System.currentTimeMillis();
        if (isTraceEnabled) {
          logger.trace("{}: Peek current time: {}", this, currentTime);
        }
        if (timeToWait == -1 || (end <= currentTime)) {
          if (isTraceEnabled) {
            logger.trace("{}: Peek breaking", this);
          }
          break;
        }

        if (isTraceEnabled) {
          logger.trace("{}: Peek continuing", this);
        }
        // Sleep a bit before trying again.
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        continue;
      }
    }
    if (batch.size() > 0) {
      peekEventsFromIncompleteTransactions(batch, lastKey);
    }

    if (isTraceEnabled) {
      logger.trace("{}: Peeked a batch of {} entries", this, batch.size());
    }
    return batch;
    // OFFHEAP: all returned AsyncEvent end up being removed from queue after the batch is sent
    // so no need to worry about off-heap refCount.
  }

  @VisibleForTesting
  void peekEventsFromIncompleteTransactions(List<AsyncEvent<?, ?>> batch, long lastKey) {
    if (!mustGroupTransactionEvents()) {
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

  protected boolean mustGroupTransactionEvents() {
    return sender.mustGroupTransactionEvents();
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

  @Override
  public String toString() {
    return "SerialGatewaySender queue :" + regionName;
  }

  @Override
  public int size() {
    int size = ((LocalRegion) region).entryCount();
    return size + sender.getTmpQueuedEventSize();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void addCacheListener(CacheListener listener) {
    AttributesMutator mutator = region.getAttributesMutator();
    mutator.addCacheListener(listener);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void removeCacheListener() {
    AttributesMutator mutator = region.getAttributesMutator();
    CacheListener[] listeners = region.getAttributes().getCacheListeners();
    for (CacheListener listener : listeners) {
      if (listener instanceof SerialSecondaryGatewayListener) {
        mutator.removeCacheListener(listener);
        break;
      }
    }
  }

  private boolean removeOldEntry(Conflatable object, Long tailKey) throws CacheException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    boolean keepOldEntry = true;

    // Determine whether conflation is enabled for this queue and object
    // Conflation is enabled iff:
    // - this queue has conflation enabled
    // - the object can be conflated
    if (enableConflation && object.shouldBeConflated()) {
      if (isDebugEnabled) {
        logger.debug("{}: Conflating {} at queue index={} queue size={} head={} tail={}", this,
            object, tailKey, size(), headKey, tailKey);
      }

      // Determine whether this region / key combination is already indexed.
      // If so, it is already in the queue. Update the value in the queue and
      // set the shouldAddToQueue flag accordingly.
      String rName = object.getRegionToConflate();
      Object key = object.getKeyToConflate();
      Long previousIndex;

      synchronized (this) {
        Map<Object, Long> latestIndexesForRegion = indexes.get(rName);
        if (latestIndexesForRegion == null) {
          latestIndexesForRegion = new HashMap<>();
          indexes.put(rName, latestIndexesForRegion);
        }

        previousIndex = latestIndexesForRegion.put(key, tailKey);
      }

      if (isDebugEnabled) {
        logger.debug("{}: Adding index key={}->index={} for {} head={} tail={}", this, key, tailKey,
            object, headKey, tailKey);
      }
      // Test if the key is contained in the latest indexes map. If the key is
      // not contained in the latest indexes map, then it should be added to
      // the queue.
      //
      // It no longer matters if we remove an entry that is going out in the
      // current
      // batch, because we already put the latest value on the tail of the
      // queue, and
      // peekedIds list prevents us from removing an entry that was not peeked.
      if (previousIndex != null) {
        if (isDebugEnabled) {
          logger.debug(
              "{}: Indexes contains index={} for key={} head={} tail={} and it can be used.", this,
              previousIndex, key, headKey, tailKey);
        }
        keepOldEntry = false;
      } else {
        if (isDebugEnabled) {
          logger.debug("{}: No old entry for key={} head={} tail={} not removing old entry.", this,
              key, headKey, tailKey);
        }
        stats.incConflationIndexesMapSize();
        keepOldEntry = true;
      }

      // Replace the object's value into the queue if necessary
      if (!keepOldEntry) {
        Conflatable previous = (Conflatable) region.remove(previousIndex);
        stats.decQueueSize(1);
        if (isDebugEnabled) {
          logger.debug("{}: Previous conflatable at key={} head={} tail={}: {}", this,
              previousIndex, headKey, tailKey, previous);
          logger.debug("{}: Current conflatable at key={} head={} tail={}: {}", this, tailKey,
              headKey, tailKey, object);
          if (previous != null) {
            logger.debug(
                "{}: Removed {} and added {} for key={} head={} tail={} in queue for region={} old event={}",
                this, previous.getValueToConflate(), object.getValueToConflate(), key, headKey,
                tailKey, rName, previous);
          }
        }
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("{}: Not conflating {} queue size: {} head={} tail={}", this, object, size(),
            headKey, tailKey);
      }
    }
    return keepOldEntry;
  }

  /**
   * Does a get that gets the value without fault values in from disk.
   */
  private AsyncEvent optimalGet(Long k) {
    // Get the object at that key (to remove the index).
    LocalRegion lr = (LocalRegion) region;
    Object o = null;
    try {
      o = lr.getValueInVMOrDiskWithoutFaultIn(k);
      if (o instanceof CachedDeserializable) {
        o = ((CachedDeserializable) o).getDeserializedValue(lr, lr.getRegionEntry(k));
      }
    } catch (EntryNotFoundException ok) {
      // just return null;
    }
    // bug #46023 do not return a destroyed entry marker
    if (o == Token.TOMBSTONE) {
      o = null;
    }
    return (AsyncEvent) o;
  }

  /*
   * this must be invoked under synchronization
   */
  private void removeIndex(Long qkey) {
    // Determine whether conflation is enabled for this queue and object
    if (enableConflation) {
      // only call get after checking enableConflation for bug 40508
      Object o = optimalGet(qkey);
      if (o instanceof Conflatable) {
        Conflatable object = (Conflatable) o;
        if (object.shouldBeConflated()) {
          // Otherwise, remove the index from the indexes map.
          String rName = object.getRegionToConflate();
          Object key = object.getKeyToConflate();

          Map<Object, Long> latestIndexesForRegion = indexes.get(rName);
          if (latestIndexesForRegion != null) {
            // Remove the index.
            Long index = latestIndexesForRegion.remove(key);
            // Decrement the size if something was removed. This check is for
            // the case where failover has occurred and the entry was not put
            // into the map initially.
            if (index != null) {
              stats.decConflationIndexesMapSize();
            }
            if (logger.isDebugEnabled()) {
              if (index != null) {
                logger.debug("{}: Removed index {} for {}", this, index, object);
              }
            }
          }
        }
      }
    }
  }

  /**
   * returns true if key a is before key b. This test handles keys that have wrapped around
   *
   */
  private boolean before(long a, long b) {
    // a is before b if a < b or a>b and a MAXIMUM_KEY/2 larger than b
    // (indicating we have wrapped)
    return a < b ^ a - b > (MAXIMUM_KEY / 2);
  }

  private long inc(long value) {
    long val = value + 1;
    val = val == MAXIMUM_KEY ? 0 : val;
    return val;
  }

  private long dec(long value) {
    long val = value - 1;
    val = val == -1 ? MAXIMUM_KEY - 1 : val;
    return val;
  }


  /**
   * Clear the list of peeked keys. The next peek will start again at the head key.
   */
  public void resetLastPeeked() {
    peekedIds.clear();
    extraPeekedIds.clear();
    lastPeekedId.set(-1);
  }

  /**
   * Finds the next object after the last key peeked
   *
   */
  private Long getCurrentKey() {
    long currentKey;
    if (lastPeekedId.get() == -1) {
      currentKey = getHeadKey();
    } else {
      currentKey = inc(lastPeekedId.get());
    }
    return currentKey;
  }

  private AsyncEvent getObjectInSerialSenderQueue(Long currentKey) {
    AsyncEvent object = optimalGet(currentKey);
    if ((null != object) && logger.isDebugEnabled()) {
      logger.debug("{}: Peeked {}->{}", this, currentKey, object);
    }
    if (object != null && object instanceof GatewaySenderEventImpl) {
      GatewaySenderEventImpl copy = ((GatewaySenderEventImpl) object).makeHeapCopyIfOffHeap();
      if (copy == null) {
        logger.debug(
            "Unable to make heap copy and will not be added to peekedIds for object" + " : {} ",
            object.toString());
      }
      object = copy;
    }
    return object;
  }

  @VisibleForTesting
  static class KeyAndEventPair {
    public final long key;
    public final AsyncEvent event;

    KeyAndEventPair(Long key, AsyncEvent event) {
      this.key = key;
      this.event = event;
    }
  }

  @VisibleForTesting
  public KeyAndEventPair peekAhead() throws CacheException {
    AsyncEvent object = null;
    Long currentKey = getCurrentKey();
    if (currentKey == null) {
      return null;
    }

    // It's important here that we check where the current key
    // is in relation to the tail key before we check to see if the
    // object exists. The reason is that the tail key is basically
    // the synchronization between this thread and the putter thread.
    // The tail key will not be incremented until the object is put in the
    // region
    // If we check for the object, and then check the tail key, we could
    // skip objects.

    // TODO: don't do a get which updates the lru, instead just get the value
    // w/o modifying the LRU.
    // Note: getting the serialized form here (if it has overflowed to disk)
    // does not save anything since GatewayBatchOp needs to GatewayEventImpl
    // in object form.
    while (before(currentKey, getTailKey())) {
      if (!extraPeekedIds.contains(currentKey)) {
        object = getObjectInSerialSenderQueue(currentKey);
        if (object != null) {
          break;
        }
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Trying head key + offset: {}", this, currentKey);
      }
      currentKey = inc(currentKey);
      // When mustGroupTransactionEvents is true, conflation cannot be enabled.
      // Therefore, if we reach here, it would not be due to a conflated event
      // but rather to an extra peeked event already sent.
      if (!mustGroupTransactionEvents() && stats != null) {
        stats.incEventsNotQueuedConflated();
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Peeked {}->{}", this, currentKey, object);
    }

    if (object != null) {
      peekedIds.add(currentKey);
      lastPeekedId.set(currentKey);
      return new KeyAndEventPair(currentKey, object);
    }
    return null;
  }

  private List<KeyAndEventPair> peekEventsWithTransactionId(TransactionId transactionId,
      long lastKey) {
    Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
        x -> transactionId.equals(x.getTransactionId());
    Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
        GatewaySenderEventImpl::isLastEventInTransaction;

    return getElementsMatching(hasTransactionIdPredicate, isLastEventInTransactionPredicate,
        lastKey);
  }

  /**
   * This method returns a list of objects that fulfill the matchingPredicate
   * If a matching object also fulfills the endPredicate then the method
   * stops looking for more matching objects.
   */
  List<KeyAndEventPair> getElementsMatching(Predicate<GatewaySenderEventImpl> condition,
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

  /**
   * Returns the value of the tail key. The tail key points to an empty where the next queue entry
   * will be stored.
   *
   * @return the value of the tail key
   */
  private long getTailKey() throws CacheException {
    long tlKey;
    // Test whether tailKey = -1. If so, the queue has just been created.
    // Go into the region to get the value of TAIL_KEY. If it is null, then
    // this is the first attempt to access this queue. Set the tailKey and
    // tailKey appropriately (to 0). If there is a value in the region, then
    // this queue has been accessed before and this instance is taking up where
    // a previous one left off. Set the tailKey to the value in the region.
    // From now on, this queue will use the value of tailKey in the VM to
    // determine the tailKey. If the tailKey != -1, set the tailKey
    // to the value of the tailKey.
    initializeKeys();

    tlKey = tailKey.get();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Determined tail key: {}", this, tlKey);
    }
    return tlKey;
  }

  /**
   * Increments the value of the tail key by one.
   *
   */
  private void incrementTailKey() throws CacheException {
    tailKey.set(inc(tailKey.get()));
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Incremented TAIL_KEY for region {} to {}", this, region.getName(),
          tailKey);
    }
  }

  /**
   * If the keys are not yet initialized, initialize them from the region .
   *
   * TODO - We could initialize the indexes maps at the time here. However, that would require
   * iterating over the values of the region rather than the keys, which could be much more
   * expensive if the region has overflowed to disk.
   *
   * We do iterate over the values of the region in SerialGatewaySender at the time of failover. see
   * SerialGatewaySender.handleFailover. So there's a possibility we can consolidate that code with
   * this method and iterate over the region once.
   *
   */
  private void initializeKeys() throws CacheException {
    if (tailKey.get() != -1) {
      return;
    }
    synchronized (this) {
      long largestKey = -1;
      long largestKeyLessThanHalfMax = -1;
      long smallestKey = -1;
      long smallestKeyGreaterThanHalfMax = -1;

      Set<Long> keySet = region.keySet();
      for (Long key : keySet) {
        long k = key.longValue();
        if (k > largestKey) {
          largestKey = k;
        }
        if (k > largestKeyLessThanHalfMax && k < MAXIMUM_KEY / 2) {
          largestKeyLessThanHalfMax = k;
        }

        if (k < smallestKey || smallestKey == -1) {
          smallestKey = k;
        }
        if ((k < smallestKeyGreaterThanHalfMax || smallestKeyGreaterThanHalfMax == -1)
            && k > MAXIMUM_KEY / 2) {
          smallestKeyGreaterThanHalfMax = k;
        }
      }

      // Test to see if the current set of keys has keys that are
      // both before and after we wrapped around the MAXIMUM_KEY
      // If we do have keys that wrapped, the
      // head key should be something close to MAXIMUM_KEY
      // and the tail key should be something close to 0.
      // Here, I'm guessing that the head key should be greater than
      // MAXIMUM_KEY/2
      // and the head key - tail key > MAXIMUM/2.
      if (smallestKeyGreaterThanHalfMax != -1 && largestKeyLessThanHalfMax != -1
          && (smallestKeyGreaterThanHalfMax - largestKeyLessThanHalfMax) > MAXIMUM_KEY / 2) {
        headKey = smallestKeyGreaterThanHalfMax;
        tailKey.set(inc(largestKeyLessThanHalfMax));
        logger.info("{}: During failover, detected that keys have wrapped tailKey={} headKey={}",
            new Object[] {this, tailKey, Long.valueOf(headKey)});
      } else {
        headKey = smallestKey == -1 ? 0 : smallestKey;
        tailKey.set(inc(largestKey));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initialized tail key to: {}, head key to: {}", this, tailKey,
            headKey);
      }
    }
  }

  /**
   * Returns the value of the head key. The head key points to the next entry to be removed from the
   * queue.
   *
   * @return the value of the head key
   */
  private long getHeadKey() throws CacheException {
    long hKey;
    // Test whether _headKey = -1. If so, the queue has just been created.
    // Go into the region to get the value of HEAD_KEY. If it is null, then
    // this is the first attempt to access this queue. Set the _headKey and
    // headKey appropriately (to 0). If there is a value in the region, then
    // this queue has been accessed before and this instance is taking up where
    // a previous one left off. Set the _headKey to the value in the region.
    // From now on, this queue will use the value of _headKey in the VM to
    // determine the headKey. If the _headKey != -1, set the headKey
    // to the value of the _headKey.
    initializeKeys();
    hKey = headKey;
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Determined head key: {}", this, hKey);
    }
    return hKey;
  }

  /**
   * Increments the value of the head key by one.
   *
   */
  private void updateHeadKey(long destroyedKey) throws CacheException {
    headKey = inc(destroyedKey);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Incremented HEAD_KEY for region {} to {}", this, region.getName(),
          headKey);
    }
  }

  /**
   * Initializes the <code>Region</code> backing this queue. The <code>Region</code>'s scope is
   * DISTRIBUTED_NO_ACK and mirror type is KEYS_VALUES and is set to overflow to disk based on the
   * <code>GatewayQueueAttributes</code>.
   *
   * @param sender The GatewaySender <code>SerialGatewaySenderImpl</code>
   * @param listener The GemFire <code>CacheListener</code>. The <code>CacheListener</code> can be
   *        null.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void initializeRegion(AbstractGatewaySender sender, CacheListener listener) {
    final InternalCache gemCache = sender.getCache();
    region = gemCache.getRegion(regionName);
    if (region == null) {
      RegionShortcut regionShortcut;
      if (enablePersistence) {
        regionShortcut = RegionShortcut.REPLICATE_PERSISTENT;
      } else {
        regionShortcut = RegionShortcut.REPLICATE;
      }
      InternalRegionFactory<Long, AsyncEvent> factory =
          gemCache.createInternalRegionFactory(regionShortcut);
      if (NO_ACK) {
        factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("The policy of region is {}",
            (enablePersistence ? DataPolicy.PERSISTENT_REPLICATE : DataPolicy.REPLICATE));
      }
      // Set listener if it is not null. The listener will be non-null
      // when the user of this queue is a secondary VM.
      if (listener != null) {
        factory.addCacheListener(listener);
      }
      // allow for no overflow directory
      EvictionAttributes ea = EvictionAttributes.createLIFOMemoryAttributes(maximumQueueMemory,
          EvictionAction.OVERFLOW_TO_DISK);

      factory.setEvictionAttributes(ea);
      factory.setConcurrencyChecksEnabled(false);

      factory.setDiskStoreName(diskStoreName);

      // In case of persistence write to disk sync and in case of eviction write in async
      factory.setDiskSynchronous(isDiskSynchronous);

      // Create the region
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Attempting to create queue region: {}", this, regionName);
      }
      final RegionAttributes<Long, AsyncEvent> ra = factory.getCreateAttributes();
      try {
        SerialGatewaySenderQueueMetaRegion meta =
            metaRegionFactory.newMetaRegion(gemCache, regionName, ra, sender);
        factory
            .setInternalMetaRegion(meta)
            .setDestroyLockFlag(true)
            .setSnapshotInputStream(null)
            .setImageTarget(null)
            .setIsUsedForSerialGatewaySenderQueue(true)
            .setInternalRegion(true)
            .setSerialGatewaySender(sender);
        region = factory.create(regionName);
        // Add overflow statistics to the mbean
        addOverflowStatisticsToMBean(gemCache, sender);

        if (logger.isDebugEnabled()) {
          logger.debug("{}: Created queue region: {}", this, region);
        }
      } catch (CacheException e) {
        logger.fatal(String.format("%s: The queue region named %s could not be created",
            this, regionName),
            e);
      }
    } else {
      if (listener != null) {
        addCacheListener(listener);
      }
    }
    if ((region != null) && cleanQueues) {
      region.clear();
    }
  }

  @VisibleForTesting
  protected void addOverflowStatisticsToMBean(Cache cache, AbstractGatewaySender sender) {
    // Get the appropriate mbean and add the overflow stats to it
    LocalRegion lr = (LocalRegion) region;
    ManagementService service = ManagementService.getManagementService(cache);
    if (sender.getId().contains(AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX)) {
      AsyncEventQueueMBean bean = (AsyncEventQueueMBean) service.getLocalAsyncEventQueueMXBean(
          AsyncEventQueueImpl.getAsyncEventQueueIdFromSenderId(sender.getId()));

      if (bean != null) {
        // Add the eviction stats
        bean.getBridge().addOverflowStatistics(lr.getEvictionStatistics());

        // Add the disk region stats
        bean.getBridge().addOverflowStatistics(lr.getDiskRegion().getStats().getStats());
      }
    } else {
      GatewaySenderMBean bean =
          (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(sender.getId());

      if (bean != null) {
        // Add the eviction stats
        bean.getBridge().addOverflowStatistics(lr.getEvictionStatistics());

        // Add the disk region stats
        bean.getBridge().addOverflowStatistics(lr.getDiskRegion().getStats().getStats());
      }
    }
  }

  public void cleanUp() {
    if (removalThread != null) {
      removalThread.shutdown();
    }
  }

  public boolean isRemovalThreadAlive() {
    if (removalThread != null) {
      return removalThread.isAlive();
    }
    return false;
  }

  @Override
  public void close() {
    Region r = getRegion();
    if (r != null && !r.isDestroyed()) {
      try {
        r.close();
      } catch (RegionDestroyedException ignored) {
      }
    }

  }

  private class BatchRemovalThread extends Thread {
    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final InternalCache cache;

    /**
     * Constructor : Creates and initializes the thread
     *
     */
    public BatchRemovalThread(InternalCache c) {
      setDaemon(true);
      cache = c;
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }
      return cache.getCancelCriterion().isCancelInProgress();
    }

    @Override
    public void run() {
      InternalDistributedSystem ids = cache.getInternalDistributedSystem();

      try { // ensure exit message is printed
        // Long waitTime = Long.getLong(QUEUE_REMOVAL_WAIT_TIME, 1000);
        for (;;) {
          try { // be somewhat tolerant of failures
            if (checkCancelled()) {
              break;
            }

            // TODO : make the thread running time configurable
            boolean interrupted = Thread.interrupted();
            try {
              synchronized (this) {
                wait(messageSyncInterval * 1000L);
              }
            } catch (InterruptedException e) {
              interrupted = true;
              if (checkCancelled()) {
                break;
              }

              break; // desperation...we must be trying to shut down...?
            } finally {
              // Not particularly important since we're exiting the thread,
              // but following the pattern is still good practice...
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }

            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread about to send the last Dispatched key {}",
                  lastDispatchedKey);
            }

            long temp;
            synchronized (SerialGatewaySenderQueue.this) {
              temp = lastDispatchedKey;
              boolean wasEmpty = temp == lastDestroyedKey;
              while (lastDispatchedKey == lastDestroyedKey) {
                SerialGatewaySenderQueue.this.wait();
                temp = lastDispatchedKey;
              }
              if (wasEmpty) {
                continue;
              }
            }
            // release not needed since disallowOffHeapValues called
            EntryEventImpl event = EntryEventImpl.create((LocalRegion) region, Operation.DESTROY,
                (lastDestroyedKey + 1), null/* newValue */, null, false, cache.getMyId());
            event.disallowOffHeapValues();
            event.setTailKey(temp);

            BatchDestroyOperation op = new BatchDestroyOperation(event);
            op.distribute();
            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread completed destroy of keys from {} to {}",
                  lastDestroyedKey, temp);
            }
            lastDestroyedKey = temp;

          } // be somewhat tolerant of failures
          catch (CancelException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread is exiting due to cancellation");
            }
            break;
          } catch (VirtualMachineError err) {
            SystemFailure.initiateFailure(err);
            // If this ever returns, rethrow the error. We're poisoned
            // now, so don't let this thread continue.
            throw err;
          } catch (Throwable t) {
            // Whenever you catch Error or Throwable, you must also
            // catch VirtualMachineError (see above). However, there is
            // _still_ a possibility that you are dealing with a cascading
            // error condition, so you also need to check to see if the JVM
            // is still usable:
            SystemFailure.checkFailure();
            if (checkCancelled()) {
              break;
            }
            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread: ignoring exception", t);
            }
          }
        } // for
      } // ensure exit message is printed
      catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("BatchRemovalThread exiting due to cancellation: " + e);
        }
      } finally {
        logger.info("The QueueRemovalThread is done.");
      }
    }

    /**
     * shutdown this thread and the caller thread will join this thread
     */
    public void shutdown() {
      shutdown = true;
      interrupt();
      boolean interrupted = Thread.interrupted();
      try {
        join(15 * 1000);
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (isAlive()) {
        logger.warn("QueueRemovalThread ignored cancellation");
      }
    }
  }

  @VisibleForTesting
  long getLastPeekedId() {
    return lastPeekedId.get();
  }

  @VisibleForTesting
  Set<Long> getExtraPeekedIds() {
    return Collections.unmodifiableSet(extraPeekedIds);
  }

  public static class SerialGatewaySenderQueueMetaRegion extends DistributedRegion {
    AbstractGatewaySender sender = null;

    protected SerialGatewaySenderQueueMetaRegion(String regionName, RegionAttributes attrs,
        LocalRegion parentRegion, InternalCache cache, AbstractGatewaySender sender,
        StatisticsClock statisticsClock) {
      super(regionName, attrs, parentRegion, cache,
          new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
              .setSnapshotInputStream(null).setImageTarget(null)
              .setIsUsedForSerialGatewaySenderQueue(true).setSerialGatewaySender(sender),
          statisticsClock);
      this.sender = sender;
    }

    // Prevent this region from using concurrency checks
    @Override
    protected boolean supportsConcurrencyChecks() {
      return false;
    }

    @Override
    public boolean isCopyOnRead() {
      return false;
    }

    // Prevent this region from participating in a TX, bug 38709
    @Override
    public boolean isSecret() {
      return true;
    }

    // @override event tracker not needed for this type of region
    @Override
    public EventTracker createEventTracker() {
      return NonDistributedEventTracker.getInstance();
    }

    @Override
    public boolean shouldNotifyBridgeClients() {
      return false;
    }

    @Override
    public boolean generateEventID() {
      return false;
    }

    @Override
    protected boolean isUsedForSerialGatewaySenderQueue() {
      return true;
    }

    @Override
    public AbstractGatewaySender getSerialGatewaySender() {
      return sender;
    }

    @Override
    public void closeEntries() {
      OffHeapClearRequired.doWithOffHeapClear(
          SerialGatewaySenderQueueMetaRegion.super::closeEntries);
    }

    @Override
    public Set<VersionSource> clearEntries(final RegionVersionVector rvv) {
      final AtomicReference<Set<VersionSource>> result = new AtomicReference<>();
      OffHeapClearRequired.doWithOffHeapClear(
          () -> result.set(SerialGatewaySenderQueueMetaRegion.super.clearEntries(rvv)));
      return result.get();
    }

    @Override
    public void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
        Object expectedOldValue)
        throws EntryNotFoundException, CacheWriterException, TimeoutException {
      try {
        super.basicDestroy(event, cacheWrite, expectedOldValue);
      } finally {
        GatewaySenderEventImpl.release(event.getRawOldValue());
      }
    }

    @Override
    public boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
        Object expectedOldValue, boolean requireOldValue, long lastModified,
        boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModificaiton)
        throws TimeoutException, CacheWriterException {
      try {
        boolean success = super.virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
            lastModified, overwriteDestroyed, invokeCallbacks, throwConcurrentModificaiton);
        if (!success) {
          // release offheap reference if GatewaySenderEventImpl is not put into
          // the region queue
          GatewaySenderEventImpl.release(event.getRawNewValue());
        }
        return success;
      } finally {
        // GatewaySenderQueue probably only adding new events into the queue.
        // Add the finally block just in case if there actually is an update
        // in the sender queue or occurs in the the future.
        GatewaySenderEventImpl.release(event.getRawOldValue());
      }
    }
  }

  static class MetaRegionFactory {
    SerialGatewaySenderQueueMetaRegion newMetaRegion(InternalCache cache,
        final String regionName,
        final RegionAttributes ra,
        AbstractGatewaySender sender) {
      SerialGatewaySenderQueueMetaRegion meta =
          new SerialGatewaySenderQueueMetaRegion(regionName, ra, null, cache, sender,
              sender.getStatisticsClock());
      return meta;
    }
  }

  public String displayContent() {
    return region.keySet().toString();
  }
}
