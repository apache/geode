/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractBucketRegionQueue extends BucketRegion {
  protected static final Logger logger = LogService.getLogger();
  
  /**
    * The maximum size of this single queue before we start blocking puts
    * The system property is in megabytes.
    */
  private final long maximumSize = 1024 * 1024 * Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "GATEWAY_QUEUE_THROTTLE_SIZE_MB", -1);
  private final long throttleTime = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "GATEWAY_QUEUE_THROTTLE_TIME_MS", 100);
  
  private final LRUStatistics stats;
  
  private final ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();
  
  private final GatewaySenderStats gatewaySenderStats;
  
  protected volatile boolean initialized = false;
  
  /**
   * Holds keys for those events that were not found in BucketRegionQueue during 
   * processing of ParallelQueueRemovalMessage. This can occur due to the scenario
   * mentioned in #49196.
   */
  private final ConcurrentHashSet<Object> failedBatchRemovalMessageKeys = 
    new ConcurrentHashSet<Object>();

  public AbstractBucketRegionQueue(String regionName, RegionAttributes attrs,
      LocalRegion parentRegion, GemFireCacheImpl cache,
      InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
    this.stats = ((AbstractLRURegionMap) getRegionMap()).getLRUStatistics();
    gatewaySenderStats = this.getPartitionedRegion().getParallelGatewaySender()
        .getStatistics();
  }
  
//Prevent this region from using concurrency checks 
  @Override
  public boolean supportsConcurrencyChecks() {
    return false;
  }

  protected void waitIfQueueFull() {
    if (maximumSize <= 0) {
      return;
    }

    // Make the put block if the queue has reached the maximum size
    // If the queue is over the maximum size, the put will wait for
    // the given throttle time until there is space in the queue
    if (stats.getCounter() > maximumSize) {
      try {
        synchronized (this.stats) {
          this.stats.wait(throttleTime);
        }
      } catch (InterruptedException e) {
        // If the thread is interrupted, just continue on
        Thread.currentThread().interrupt();
      }
    }
  }

  protected void notifyEntriesRemoved() {
    if (maximumSize > 0) {
      synchronized (this.stats) {
        this.stats.notifyAll();
      }
    }
  }
  
  @Override
  protected void distributeUpdateOperation(EntryEventImpl event,
      long lastModified) {
    /**
     * no-op as there is no need to distribute this operation.
     */
  }

  // TODO: Kishor: While merging below nethod is defined as skipWriteLock.
  // Actually Cedar uses needWriteLock. Hence changed method to need writelock.
  // We have to make it consistecnt. Either skipSwriteLock or needWriteLock
//  /**
//   * In case of update we need not take lock as we are doing local
//   * operation.
//   * After BatchRemovalThread: We don't need lock for destroy as well. 
//   * @param event
//   * @return if we can skip taking the lock or not
//   */
//  
//  protected boolean skipWriteLock(EntryEventImpl event) {
//    return true;
//  }
  
  
  protected boolean needWriteLock(EntryEventImpl event) {
    return false;
  }
  
  @Override
  protected long basicPutPart2(EntryEventImpl event, RegionEntry entry,
      boolean isInitialized, long lastModified, boolean clearConflict) {
    return System.currentTimeMillis();
  }
  
  @Override
  protected void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event) {
    /**
     * We are doing local destroy on this bucket. No need to send destroy
     * operation to remote nodes.
     */
    if (logger.isDebugEnabled()) {
      logger.debug("For Key {}, BasicDestroyBeforeRemoval: no need to send destroy operation to remote nodes. This will be done using BatchRemoval Message.",
        event.getKey());
    }
  }

  @Override
  protected void distributeDestroyOperation(EntryEventImpl event) {
    /**
     * no-op as there is no need to distribute this operation.
     */
  }

  /**
   * Overridden to allow clear operation on ShadowBucketRegion. Do nothing here,
   * so clear operations proceeds smoothly.
   */
  @Override
  protected void updateSizeOnClearRegion(int sizeBeforeClear) {
    
  }
  
  /**
   * @return the initializationLock
   */
  public ReentrantReadWriteLock getInitializationLock() {
    return initializationLock;
  }
    /**
   * Does a get that attempts to not fault values in from disk or make the entry
   * the most recent in the LRU.
   */
/*  protected Object optimalGet(Object k) {
    // Get the object at that key (to remove the index).
    Object object = null;
    try {
      object = getValueInVM(k); // OFFHEAP deserialize
      if (object == null) {
        // must be on disk
        // fault it in w/o putting it back in the region
        object = getValueOnDiskOrBuffer(k);
        if (object == null) {
          // try memory one more time in case it was already faulted back in
          object = getValueInVM(k); // OFFHEAP deserialize
          if (object == null) {
            // if we get this far give up and just do a get
            object = get(k);
          } else {
            if (object instanceof CachedDeserializable) {
              object = ((CachedDeserializable)object).getDeserializedValue(
                  this, this.getRegionEntry(k));
            }
          }
        }
      } else {
        if (object instanceof CachedDeserializable) {
          object = ((CachedDeserializable)object).getDeserializedValue(this,
              this.getRegionEntry(k));
        }
      }
    } catch (EntryNotFoundException ok) {
      // just return null;
    }
    if (object == Token.TOMBSTONE) {
      object = null;
    }

    return object;
  }*/
  
  public void destroyKey(Object key) throws ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug(" destroying primary key {}", key);
    }
    @Released EntryEventImpl event = getPartitionedRegion().newDestroyEntryEvent(key,
        null);
    event.setEventId(new EventID(cache.getSystem()));
    try {
      event.setRegion(this);
      basicDestroy(event, true, null);
      checkReadiness();
    } catch (EntryNotFoundException enf) {
      if (getPartitionedRegion().isDestroyed()) {
        getPartitionedRegion().checkReadiness();
        if (isBucketDestroyed()) {
          throw new ForceReattemptException(
              "Bucket moved",
              new RegionDestroyedException(
                  LocalizedStrings.PartitionedRegionDataStore_REGION_HAS_BEEN_DESTROYED
                      .toLocalizedString(), getPartitionedRegion()
                      .getFullPath()));
        }
      }
      throw enf;
    } catch (RegionDestroyedException rde) {
      getPartitionedRegion().checkReadiness();
      if (isBucketDestroyed()) {
        throw new ForceReattemptException("Bucket moved while destroying key "
            + key, rde);
      }
    } finally {
      event.release();
    }

    this.notifyEntriesRemoved();
  }
  
  public void decQueueSize(int size) {
    this.gatewaySenderStats.decQueueSize(size);
  }
  
  public void decQueueSize() {
    this.gatewaySenderStats.decQueueSize();
  }

  public void incQueueSize(int size) {
    this.gatewaySenderStats.incQueueSize(size);
  }
  
  public void incQueueSize() {
    this.gatewaySenderStats.incQueueSize();
  }
  
  protected void loadEventsFromTempQueue() {
    if (logger.isDebugEnabled()) {
      logger.debug("For bucket {} about to load events from the temp queue...", getId());
    }
    Set queues = this.getPartitionedRegion().getParallelGatewaySender()
        .getQueues();
    if (queues != null) {
      ConcurrentParallelGatewaySenderQueue prq = (ConcurrentParallelGatewaySenderQueue)queues
          .toArray()[0];
      // synchronized (prq.getBucketToTempQueueMap()) {
      BlockingQueue<GatewaySenderEventImpl> tempQueue = prq.getBucketTmpQueue(getId());
         // .getBucketToTempQueueMap().get(getId());
      if (tempQueue != null && !tempQueue.isEmpty()) {
        synchronized (tempQueue) {
          try {
          //ParallelQueueRemovalMessage checks for the key in BucketRegionQueue
          //and if not found there, it removes it from tempQueue. When tempQueue 
          //is getting loaded in BucketRegionQueue, it may not find the key in both.
          //To fix this race, load the events in writeLock.
          getInitializationLock().writeLock().lock();
          // add the events from tempQueue to the region
          GatewaySenderEventImpl event;
          while ((event = tempQueue.poll()) != null) {
            try {
              event.setPossibleDuplicate(true);
              if (this.addToQueue(event.getShadowKey(), event)) {
                event = null;
              }
            }
            catch (ForceReattemptException e) {
              if (logger.isDebugEnabled()) {
                logger.debug("For bucket {} , enqueing event {} caused exception", getId(), event, e);
              }
            } finally {
              if (event != null) {
                event.release();
              }
            }
          }
          } finally {
            if (!tempQueue.isEmpty()) {
              for (GatewaySenderEventImpl e: tempQueue) {
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
   * Marks batchSize number of events in the iterator as duplicate 
   */
  protected void markEventsAsDuplicate(int batchSize, Iterator itr) {
    int i = 0;
    // mark number of event equal to the batchSize for setPossibleDuplicate to
    // true before this bucket becomes primary on the node
    while (i < batchSize && itr.hasNext()) {
      Object key = itr.next();
      Object senderEvent = 
          getNoLRU(key, true, false, false);
      
      if (senderEvent != null) {
        ((GatewaySenderEventImpl)senderEvent).setPossibleDuplicate(true);
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
  protected boolean virtualPut(EntryEventImpl event, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      long lastModified, boolean overwriteDestroyed) throws TimeoutException,
      CacheWriterException {
    boolean success = super.virtualPut(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);
    if (success) {
      if (logger.isDebugEnabled()) {
        logger.debug("Key : ----> {}", event.getKey());
      }
      //@Unretained Object ov = event.getRawOldValue();
      //if (ov instanceof GatewaySenderEventImpl) {
      //  ((GatewaySenderEventImpl)ov).release();
      //}
       GatewaySenderEventImpl.release(event.getRawOldValue());
    }
    return success;
    
  }
  @Override
  protected void basicDestroy(final EntryEventImpl event,
      final boolean cacheWrite, Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {
    super.basicDestroy(event, cacheWrite, expectedOldValue);
    //@Unretained Object rov = event.getRawOldValue();
    //if (rov instanceof GatewaySenderEventImpl) {
    //  ((GatewaySenderEventImpl) rov).release();
    //}
	GatewaySenderEventImpl.release(event.getRawOldValue());
  }


  /**
   * Return all of the user PR buckets for this bucket region queue.
   */
  public Collection<BucketRegion> getCorrespondingUserPRBuckets() {
    List<BucketRegion> userPRBuckets = new ArrayList<BucketRegion>(4);
    Map<String, PartitionedRegion> colocatedPRs = ColocationHelper
        .getAllColocationRegions(getPartitionedRegion());
    for (PartitionedRegion colocatedPR : colocatedPRs.values()) {
      if (!colocatedPR.isShadowPR() && isThisSenderAttached(colocatedPR)) {
        BucketRegion parentBucket = colocatedPR.getDataStore()
            .getLocalBucketById(getId());
        if (parentBucket != null)
          userPRBuckets.add(parentBucket);
      }
    }
    return userPRBuckets;
  }

  private boolean isThisSenderAttached(PartitionedRegion pr) {
    return pr.getParallelGatewaySenderIds().contains(
        getPartitionedRegion().getParallelGatewaySender().getId());
  }
  
  /**
   * It should be an atomic operation. If the key has been added to the
   * eventSeqNumQueue then make sure that the value is in the Bucket before the
   * eventSeqNumQueue is available for peek/remove/take from other thread.
   * 
   * @param key
   * @param value
   * @return boolean which shows whether the operation was successful or not.
   * @throws ForceReattemptException
   */
  public boolean addToQueue(Object key, Object value)
      throws ForceReattemptException {
    
    //if the key exists in failedBatchRemovalMessageKeys, then 
    //remove it from there and return. Handling of a scenario in #49196.
    if (failedBatchRemovalMessageKeys.remove(key)) {
      return false;
    }
    
    boolean didPut = false;
    long startPut = CachePerfStats.getStatTime();
    // Value will always be an instanceof GatewaySenderEventImpl which
    // is never stored offheap so this EntryEventImpl values will never be off-heap.
    // So the value that ends up being stored in this region is a GatewaySenderEventImpl
    // which may have a reference to a value stored off-heap.
    EntryEventImpl event = EntryEventImpl.create(this, Operation.UPDATE, key,
        value, null, false, getMyId());
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
    
    //check again if the key exists in failedBatchRemovalMessageKeys, 
    //if yes, then remove it from there and destroy the key from BucketRegionQueue. 
    //This is to reduce the window of race condition described by Darrel in #49196.
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
        ConcurrentParallelGatewaySenderQueue queue = (ConcurrentParallelGatewaySenderQueue)ep.getQueue();
        if (logger.isDebugEnabled()) {
          logger.debug("notifyEventProcessor : {} event processor {} queue {}", sender, ep, queue);  
        }
        queue.notifyEventProcessorIfRequired(this.getId());
      }
    }
  }
  
  public boolean isInitialized() {
    return this.initialized;
  }
  
  /**
   * 
   * @param key
   */
  public void addToFailedBatchRemovalMessageKeys(Object key) {
    failedBatchRemovalMessageKeys.add(key);
  }
  
  public ConcurrentHashSet<Object> getFailedBatchRemovalMessageKeys() {
	  return this.failedBatchRemovalMessageKeys;
  }
  
}
