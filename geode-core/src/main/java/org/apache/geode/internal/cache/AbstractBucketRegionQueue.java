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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public abstract class AbstractBucketRegionQueue extends BucketRegion {
  protected static final Logger logger = LogService.getLogger();

  /**
   * The maximum size of this single queue before we start blocking puts The system property is in
   * megabytes.
   */
  private final long maximumSize = 1024 * 1024
      * Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "GATEWAY_QUEUE_THROTTLE_SIZE_MB", -1);
  private final long throttleTime =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "GATEWAY_QUEUE_THROTTLE_TIME_MS", 100);

  private final ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();

  private final GatewaySenderStats gatewaySenderStats;

  protected volatile boolean initialized = false;

  /**
   * Holds keys for those events that were not found in BucketRegionQueue during processing of
   * ParallelQueueRemovalMessage. This can occur due to the scenario mentioned in #49196.
   */
  private final Set<Object> failedBatchRemovalMessageKeys = ConcurrentHashMap.newKeySet();

  AbstractBucketRegionQueue(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs,
      StatisticsClock statisticsClock) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs, statisticsClock);
    this.gatewaySenderStats =
        this.getPartitionedRegion().getParallelGatewaySender().getStatistics();
  }

  // Prevent this region from using concurrency checks
  @Override
  protected boolean supportsConcurrencyChecks() {
    return false;
  }

  private final Object waitForEntriesToBeRemoved = new Object();

  protected void waitIfQueueFull() {
    if (maximumSize <= 0) {
      return;
    }

    // Make the put block if the queue has reached the maximum size
    // If the queue is over the maximum size, the put will wait for
    // the given throttle time until there is space in the queue
    if (getEvictionCounter() > maximumSize) {
      try {
        synchronized (this.waitForEntriesToBeRemoved) {
          this.waitForEntriesToBeRemoved.wait(throttleTime);
        }
      } catch (InterruptedException e) {
        // If the thread is interrupted, just continue on
        Thread.currentThread().interrupt();
      }
    }
  }

  protected void notifyEntriesRemoved() {
    if (maximumSize > 0) {
      synchronized (this.waitForEntriesToBeRemoved) {
        this.waitForEntriesToBeRemoved.notifyAll();
      }
    }
  }

  @Override
  protected void distributeUpdateOperation(EntryEventImpl event, long lastModified) {
    /*
     * no-op as there is no need to distribute this operation.
     */
  }

  // TODO: Kishor: While merging below nethod is defined as skipWriteLock.
  // Actually Cedar uses needWriteLock. Hence changed method to need writelock.
  // We have to make it consistecnt. Either skipSwriteLock or needWriteLock
  // /**
  // * In case of update we need not take lock as we are doing local
  // * operation.
  // * After BatchRemovalThread: We don't need lock for destroy as well.
  // * @param event
  // * @return if we can skip taking the lock or not
  // */
  //
  // protected boolean skipWriteLock(EntryEventImpl event) {
  // return true;
  // }


  @Override
  protected boolean needWriteLock(EntryEventImpl event) {
    return false;
  }

  @Override
  public long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
      long lastModified, boolean clearConflict) {
    return System.currentTimeMillis();
  }

  @Override
  public void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event) {
    /*
     * We are doing local destroy on this bucket. No need to send destroy operation to remote nodes.
     */
    if (logger.isDebugEnabled()) {
      logger.debug(
          "For Key {}, BasicDestroyBeforeRemoval: no need to send destroy operation to remote nodes. This will be done using BatchRemoval Message.",
          event.getKey());
    }
  }

  @Override
  protected void distributeDestroyOperation(EntryEventImpl event) {
    /*
     * no-op as there is no need to distribute this operation.
     */
  }

  /**
   * Overridden to allow clear operation on ShadowBucketRegion. Do nothing here, so clear operations
   * proceeds smoothly.
   */
  @Override
  void updateSizeOnClearRegion(int sizeBeforeClear) {

  }

  /**
   * @return the initializationLock
   */
  public ReentrantReadWriteLock getInitializationLock() {
    return initializationLock;
  }

  public abstract void destroyKey(Object key) throws ForceReattemptException;

  public void decQueueSize(int size) {
    this.gatewaySenderStats.decQueueSize(size);
  }

  public void decSecondaryQueueSize(int size) {
    this.gatewaySenderStats.decSecondaryQueueSize(size);
  }

  public void decQueueSize() {
    this.gatewaySenderStats.decQueueSize();
  }

  public void incQueueSize(int size) {
    this.gatewaySenderStats.incQueueSize(size);
  }

  public void incSecondaryQueueSize(int size) {
    this.gatewaySenderStats.incSecondaryQueueSize(size);
  }

  public void incEventsProcessedByPQRM(int size) {
    this.gatewaySenderStats.incEventsProcessedByPQRM(size);
  }

  public void incQueueSize() {
    this.gatewaySenderStats.incQueueSize();
  }

  protected void loadEventsFromTempQueue() {
    if (logger.isDebugEnabled()) {
      logger.debug("For bucket {} about to load events from the temp queue...", getId());
    }
    Set queues = this.getPartitionedRegion().getParallelGatewaySender().getQueues();
    if (queues != null) {
      ConcurrentParallelGatewaySenderQueue prq =
          (ConcurrentParallelGatewaySenderQueue) queues.toArray()[0];
      // synchronized (prq.getBucketToTempQueueMap()) {
      BlockingQueue<GatewaySenderEventImpl> tempQueue = prq.getBucketTmpQueue(getId());
      // .getBucketToTempQueueMap().get(getId());
      if (tempQueue != null && !tempQueue.isEmpty()) {
        synchronized (tempQueue) {
          try {
            // ParallelQueueRemovalMessage checks for the key in BucketRegionQueue
            // and if not found there, it removes it from tempQueue. When tempQueue
            // is getting loaded in BucketRegionQueue, it may not find the key in both.
            // To fix this race, load the events in writeLock.
            getInitializationLock().writeLock().lock();
            // add the events from tempQueue to the region
            GatewaySenderEventImpl event;
            while ((event = tempQueue.poll()) != null) {
              try {
                event.setPossibleDuplicate(true);
                if (this.addToQueue(event.getShadowKey(), event)) {
                  event = null;
                }
              } catch (ForceReattemptException e) {
                if (logger.isDebugEnabled()) {
                  logger.debug("For bucket {} , enqueing event {} caused exception", getId(), event,
                      e);
                }
              } finally {
                if (event != null) {
                  event.release();
                }
              }
            }
          } finally {
            if (!tempQueue.isEmpty()) {
              for (GatewaySenderEventImpl e : tempQueue) {
                e.release();
              }
              tempQueue.clear();
            }
            getInitializationLock().writeLock().unlock();
          }
        }
      }

      // }
    }
  }

  /**
   * Marks all events in the iterator as duplicate
   */
  protected void markEventsAsDuplicate(Iterator itr) {
    int i = 0;
    // mark setPossibleDuplicate to true for all events in this bucket before it becomes primary on
    // the node
    while (itr.hasNext()) {
      Object key = itr.next();
      Object senderEvent = getNoLRU(key, true, false, false);

      if (senderEvent != null) {
        ((GatewaySenderEventImpl) senderEvent).setPossibleDuplicate(true);
        if (logger.isDebugEnabled()) {
          logger.debug("Set possibleDuplicate to true on event: {}", senderEvent);
        }
      }
      i++;
    }
  }

  @Override
  public void forceSerialized(EntryEventImpl event) {
    // NOOP since we want the value in the region queue to stay in object form.
  }

  @Override
  public boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModificaiton)
      throws TimeoutException, CacheWriterException {
    try {
      boolean success = super.virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
          lastModified, overwriteDestroyed, invokeCallbacks, throwConcurrentModificaiton);
      if (success) {
        if (logger.isDebugEnabled()) {
          logger.debug("Key : ----> {}", event.getKey());
        }
      } else {
        GatewaySenderEventImpl.release(event.getRawNewValue());
      }
      return success;
    } finally {
      GatewaySenderEventImpl.release(event.getRawOldValue());
    }

  }

  /**
   * Return all of the user PR buckets for this bucket region queue.
   */
  public Collection<BucketRegion> getCorrespondingUserPRBuckets() {
    List<BucketRegion> userPRBuckets = new ArrayList<BucketRegion>(4);
    Map<String, PartitionedRegion> colocatedPRs =
        ColocationHelper.getAllColocationRegions(getPartitionedRegion());
    for (PartitionedRegion colocatedPR : colocatedPRs.values()) {
      if (!colocatedPR.isShadowPR() && isThisSenderAttached(colocatedPR)) {
        BucketRegion parentBucket = colocatedPR.getDataStore().getLocalBucketById(getId());
        if (parentBucket != null)
          userPRBuckets.add(parentBucket);
      }
    }
    return userPRBuckets;
  }

  private boolean isThisSenderAttached(PartitionedRegion pr) {
    return pr.getParallelGatewaySenderIds()
        .contains(getPartitionedRegion().getParallelGatewaySender().getId());
  }

  /**
   * It should be an atomic operation. If the key has been added to the eventSeqNumQueue then make
   * sure that the value is in the Bucket before the eventSeqNumQueue is available for
   * peek/remove/take from other thread.
   *
   * @return boolean which shows whether the operation was successful or not.
   */
  public boolean addToQueue(Object key, Object value) throws ForceReattemptException {

    // if the key exists in failedBatchRemovalMessageKeys, then
    // remove it from there and return. Handling of a scenario in #49196.
    if (failedBatchRemovalMessageKeys.remove(key)) {
      return false;
    }

    boolean didPut = false;
    long startPut = getStatisticsClock().getTime();
    // Value will always be an instanceof GatewaySenderEventImpl which
    // is never stored offheap so this EntryEventImpl values will never be off-heap.
    // So the value that ends up being stored in this region is a GatewaySenderEventImpl
    // which may have a reference to a value stored off-heap.
    EntryEventImpl event =
        EntryEventImpl.create(this, Operation.UPDATE, key, value, null, false, getMyId());
    // here avoiding unnecessary validations of key, value. Readniness check
    // will be handled in virtualPut. avoiding extractDelta as this will be new
    // entry everytime
    // EntryEventImpl event = getPartitionedRegion().newUpdateEntryEvent(key,
    // value, null);
    event.copyOffHeapToHeap();

    if (logger.isDebugEnabled()) {
      logger.debug("Value : {}", event.getRawNewValue());
    }
    waitIfQueueFull();

    try {

      didPut = virtualPut(event, false, false, null, false, startPut, true);

      checkReadiness();
    } catch (RegionDestroyedException rde) {
      // this can now happen due to a re-balance removing a bucket
      getPartitionedRegion().checkReadiness();
      if (isBucketDestroyed()) {
        throw new ForceReattemptException("Bucket moved", rde);
      }
    } finally {
      if (!didPut) {
        GatewaySenderEventImpl.release(value);
      }
    }

    // check again if the key exists in failedBatchRemovalMessageKeys,
    // if yes, then remove it from there and destroy the key from BucketRegionQueue.
    // This is to reduce the window of race condition described by Darrel in #49196.
    if (failedBatchRemovalMessageKeys.remove(key) && didPut) {
      destroyKey(key);
      didPut = false;
    } else {
      addToEventQueue(key, didPut, event);
    }
    return didPut;
  }

  @Override
  public void closeEntries() {
    OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
      @Override
      public void run() {
        AbstractBucketRegionQueue.super.closeEntries();
      }
    });
    clearQueues();

  }

  @Override
  public Set<VersionSource> clearEntries(final RegionVersionVector rvv) {
    final AtomicReference<Set<VersionSource>> result = new AtomicReference<Set<VersionSource>>();
    OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
      @Override
      public void run() {
        result.set(AbstractBucketRegionQueue.super.clearEntries(rvv));
      }
    });
    clearQueues();
    return result.get();
  }

  protected abstract void clearQueues();

  protected abstract void addToEventQueue(Object key, boolean didPut, EntryEventImpl event);

  @Override
  public void afterAcquiringPrimaryState() {
    super.afterAcquiringPrimaryState();
    notifyEventProcessor();
  }

  protected void notifyEventProcessor() {
    AbstractGatewaySender sender = getPartitionedRegion().getParallelGatewaySender();
    if (sender != null) {
      AbstractGatewaySenderEventProcessor ep = sender.getEventProcessor();
      if (ep != null) {
        ConcurrentParallelGatewaySenderQueue queue =
            (ConcurrentParallelGatewaySenderQueue) ep.getQueue();
        if (logger.isDebugEnabled()) {
          logger.debug("notifyEventProcessor : {} event processor {} queue {}", sender, ep, queue);
        }
        queue.notifyEventProcessorIfRequired(this.getId());
      }
    }
  }

  @Override
  public boolean isInitialized() {
    return this.initialized;
  }

  public void addToFailedBatchRemovalMessageKeys(Object key) {
    failedBatchRemovalMessageKeys.add(key);
  }

  public boolean isFailedBatchRemovalMessageKeysClearedFlag() {
    return failedBatchRemovalMessageKeysClearedFlag;
  }

  public void setFailedBatchRemovalMessageKeysClearedFlag(
      boolean failedBatchRemovalMessageKeysClearedFlag) {
    this.failedBatchRemovalMessageKeysClearedFlag = failedBatchRemovalMessageKeysClearedFlag;
  }

  private boolean failedBatchRemovalMessageKeysClearedFlag = false;


  public Set<Object> getFailedBatchRemovalMessageKeys() {
    return this.failedBatchRemovalMessageKeys;
  }

}
