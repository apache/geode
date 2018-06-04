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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.persistence.query.mock.ByteComparator;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.BucketRegionQueueUnavailableException;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.concurrent.Atomics;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.internal.offheap.annotations.Released;

public class BucketRegionQueue extends AbstractBucketRegionQueue {

  private static final Logger logger = LogService.getLogger();

  /**
   * The <code>Map</code> mapping the regionName->key to the queue key. This index allows fast
   * updating of entries in the queue for conflation. This is necesaary for Colocated regions and if
   * any of the regions use same key for data.
   */
  private final Map indexes;

  /**
   * A transient deque, but should be treated like as a fifo queue to maintain the eventSeqNum of
   * the events that are to be sent to remote site. It is cleared when the queue is cleared.
   */
  private final BlockingDeque<Object> eventSeqNumDeque = new LinkedBlockingDeque<Object>();

  private long lastKeyRecovered;

  private final AtomicLong latestQueuedKey = new AtomicLong();

  private final AtomicLong latestAcknowledgedKey = new AtomicLong();

  public BucketRegionQueue(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
    this.keySet();
    this.indexes = new ConcurrentHashMap<Object, Long>();
  }

  @Override
  protected void cleanUpDestroyedTokensAndMarkGIIComplete(
      InitialImageOperation.GIIStatus giiStatus) {
    // Load events from temp queued events
    loadEventsFromTempQueue();

    // Initialize the eventSeqNumQueue
    initializeEventSeqNumQueue();

    // Clean up destroyed tokens
    super.cleanUpDestroyedTokensAndMarkGIIComplete(giiStatus);
  }

  private void initializeEventSeqNumQueue() {
    getInitializationLock().writeLock().lock();
    try {
      if (!this.keySet().isEmpty()) {
        if (getPartitionedRegion().getColocatedWith() == null) {
          List<EventID> keys = new ArrayList<EventID>(this.keySet());
          Collections.sort(keys, new Comparator<EventID>() {
            @Override
            public int compare(EventID o1, EventID o2) {
              int compareMem =
                  new ByteComparator().compare(o1.getMembershipID(), o2.getMembershipID());
              if (compareMem == 1) {
                return 1;
              } else if (compareMem == -1) {
                return -1;
              } else {
                if (o1.getThreadID() > o2.getThreadID()) {
                  return 1;
                } else if (o1.getThreadID() < o2.getThreadID()) {
                  return -1;
                } else {
                  return o1.getSequenceID() < o2.getSequenceID() ? -1
                      : o1.getSequenceID() == o2.getSequenceID() ? 0 : 1;
                }
              }
            }
          });
          for (EventID eventID : keys) {
            eventSeqNumDeque.addLast(eventID);
          }
        } else {
          TreeSet<Long> sortedKeys = new TreeSet<Long>(this.keySet());
          // although the empty check for this.keySet() is done above,
          // do the same for sortedKeys as well because the keySet() might have become
          // empty since the above check was made (keys might have been destroyed through
          // BatchRemoval)
          // fix for #49679 NoSuchElementException thrown from BucketRegionQueue.initialize
          if (!sortedKeys.isEmpty()) {
            for (Long key : sortedKeys) {
              eventSeqNumDeque.addLast(key);
            }
            lastKeyRecovered = sortedKeys.last();
            if (this.getEventSeqNum() != null) {
              Atomics.setIfGreater(getEventSeqNum(), lastKeyRecovered);
            }
          }
        }

        if (logger.isDebugEnabled()) {
          logger.debug(
              "For bucket {} ,total keys recovered are : {} last key recovered is : {} and the seqNo is ",
              getId(), eventSeqNumDeque.size(), lastKeyRecovered, getEventSeqNum());
        }
      }
      this.initialized = true;
      // Now, the bucket is initialized. Destroy the failedBatchRemovalKeys.
      destroyFailedBatchRemovalMessageKeys();
    } finally {
      notifyEventProcessor();
      getInitializationLock().writeLock().unlock();
    }
  }

  /**
   * When the GII was going, some of BatchRemoval messages (especially for events destroyed due to
   * conflation) might have failed since the event might not be found in the BucketRegionQueue.
   * Those messages are added to failedBatchRemovalMessageKeys map in ParallelQueueRemovalMessage.
   * Events found in the map need to be destroyed once GII is done. Fix for #47431. This method is
   * invoked deliberately after this.initialized is set to true to fix defect #50316.
   */
  private void destroyFailedBatchRemovalMessageKeys() {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    Iterator<Object> itr = getFailedBatchRemovalMessageKeys().iterator();
    while (itr.hasNext()) {
      // at this point, failedBatchRemovalMessageKeys contains messages failed during bucket
      // initialization only. Iterate over failedBatchRemovalMessageKeys and clear it completely.
      Object key = itr.next();
      itr.remove();
      if (isDebugEnabled) {
        logger.debug("key from failedBatchRemovalMessageKeys is: {}", key);
      }
      if (containsKey(key)) {
        try {
          destroyKey(key);
          if (isDebugEnabled) {
            logger.debug("Destroyed {} from bucket: ", key, getId());
          }
        } catch (ForceReattemptException fe) {
          if (isDebugEnabled) {
            logger.debug("Bucket :{} moved to other member", getId());
          }
        }
      }
    }
  }

  @Override
  public void beforeAcquiringPrimaryState() {
    Iterator<Object> itr = eventSeqNumDeque.iterator();
    markEventsAsDuplicate(itr);
  }

  @Override
  public void closeEntries() {
    OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
      @Override
      public void run() {
        BucketRegionQueue.super.closeEntries();
      }
    });
    this.indexes.clear();
    this.eventSeqNumDeque.clear();
  }

  @Override
  public Set<VersionSource> clearEntries(final RegionVersionVector rvv) {
    final AtomicReference<Set<VersionSource>> result = new AtomicReference<Set<VersionSource>>();
    OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
      @Override
      public void run() {
        result.set(BucketRegionQueue.super.clearEntries(rvv));
      }
    });
    this.eventSeqNumDeque.clear();
    return result.get();
  }


  @Override
  public void forceSerialized(EntryEventImpl event) {
    // NOOP since we want the value in the region queue to stay in object form.
  }

  protected void clearQueues() {
    getInitializationLock().writeLock().lock();
    try {
      this.indexes.clear();
      this.eventSeqNumDeque.clear();
    } finally {
      getInitializationLock().writeLock().unlock();
    }
  }

  @Override
  public boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws TimeoutException, CacheWriterException {
    try {
      boolean success = super.virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
          lastModified, overwriteDestroyed);

      if (success) {
        if (getPartitionedRegion().getColocatedWith() == null) {
          return success;
        }

        if (getPartitionedRegion().isConflationEnabled() && this.getBucketAdvisor().isPrimary()) {
          Object object = event.getNewValue();
          Long key = (Long) event.getKey();
          if (object instanceof Conflatable) {
            if (logger.isDebugEnabled()) {
              logger.debug("Key :{} , Object : {} is conflatable", key, object);
            }
            // TODO: TO optimize by destroying on primary and secondary separately
            // in case of conflation
            conflateOldEntry((Conflatable) object, key);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Object : {} is not conflatable", object);
            }
          }
        }
      } else {
        GatewaySenderEventImpl.release(event.getRawNewValue());
      }
      return success;
    } finally {
      GatewaySenderEventImpl.release(event.getRawOldValue());
    }
  }

  private void conflateOldEntry(Conflatable object, Long tailKey) {
    PartitionedRegion region = this.getPartitionedRegion();
    Conflatable conflatableObject = object;
    if (region.isConflationEnabled() && conflatableObject.shouldBeConflated()) {
      Object keyToConflate = conflatableObject.getKeyToConflate();
      String rName = object.getRegionToConflate();
      if (logger.isDebugEnabled()) {
        logger.debug(" The region name is : {}", rName);
      }
      Map latestIndexesForRegion = (Map) this.indexes.get(rName);
      if (latestIndexesForRegion == null) {
        latestIndexesForRegion = new ConcurrentHashMap();
        this.indexes.put(rName, latestIndexesForRegion);
      }
      Long previousTailKey = (Long) latestIndexesForRegion.put(keyToConflate, tailKey);
      if (previousTailKey != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Conflating {} at queue index={} and previousTailKey={} ", this, object,
              tailKey, previousTailKey);
        }
        AbstractGatewaySenderEventProcessor ep =
            region.getParallelGatewaySender().getEventProcessor();
        if (ep == null)
          return;
        ConcurrentParallelGatewaySenderQueue queue =
            (ConcurrentParallelGatewaySenderQueue) ep.getQueue();
        // Give the actual conflation work to another thread.
        // ParallelGatewaySenderQueue takes care of maintaining a thread pool.
        queue.conflateEvent(conflatableObject, getId(), previousTailKey);
      } else {
        region.getParallelGatewaySender().getStatistics().incConflationIndexesMapSize();
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Not conflating {}", this, object);
      }
    }
  }

  // No need to synchronize because it is called from a synchronized method
  private void removeIndex(Long qkey) {
    // Determine whether conflation is enabled for this queue and object
    Object o = getNoLRU(qkey, true, false, false);
    if (o instanceof Conflatable) {
      Conflatable object = (Conflatable) o;
      if (object.shouldBeConflated()) {
        // Otherwise, remove the index from the indexes map.
        String rName = object.getRegionToConflate();
        Object key = object.getKeyToConflate();
        Map latestIndexesForRegion = (Map) this.indexes.get(rName);
        if (latestIndexesForRegion != null) {
          // Remove the index if appropriate. Verify the qKey is actually the one being referenced
          // in the index. If it isn't, then another event has been received for the real key. In
          // that case, don't remove the index since it has already been overwritten.
          if (latestIndexesForRegion.get(key) == qkey) {
            Long index = (Long) latestIndexesForRegion.remove(key);
            if (index != null) {
              this.getPartitionedRegion().getParallelGatewaySender().getStatistics()
                  .decConflationIndexesMapSize();
              if (logger.isDebugEnabled()) {
                logger.debug("{}: Removed index {} for {}", this, index, object);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
      Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {
    if (getPartitionedRegion().isConflationEnabled()) {
      removeIndex((Long) event.getKey());
    }
    try {
      super.basicDestroy(event, cacheWrite, expectedOldValue);
    } finally {
      GatewaySenderEventImpl.release(event.getRawOldValue());
    }

    // Primary buckets should already remove the key while peeking
    if (!this.getBucketAdvisor().isPrimary()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" removing the key {} from eventSeqNumQueue", event.getKey());
      }
      this.eventSeqNumDeque.remove(event.getKey());
    }
  }

  /**
   * Does a get that gets the value without fault values in from disk.
   */
  private Object optimalGet(Object k) {
    // Get the object at that key (to remove the index).
    Object object = null;
    try {
      object = getValueInVMOrDiskWithoutFaultIn(k);
      if (object != null && object instanceof CachedDeserializable) {
        object = ((CachedDeserializable) object).getDeserializedValue(this, this.getRegionEntry(k));
      }
    } catch (EntryNotFoundException ok) {
      // just return null;
    }
    if (object == Token.TOMBSTONE) {
      object = null;
    }
    return object; // OFFHEAP: ok since callers are careful to do destroys on region queue after
                   // finished with peeked object.
  }

  public Object peek() {
    Object key = null;
    Object object = null;
    // doing peek in initializationLock because during region destroy, the clearQueues
    // clears the eventSeqNumQueue and can cause data inconsistency (defect #48984)
    getInitializationLock().readLock().lock();
    try {
      if (this.getPartitionedRegion().isDestroyed()) {
        throw new BucketRegionQueueUnavailableException();
      }
      key = this.eventSeqNumDeque.peekFirst();
      if (key != null) {
        object = optimalGet(key);
        if (object == null && !this.getPartitionedRegion().isConflationEnabled()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "The value against key {} in the bucket region queue with id {} is NULL for the GatewaySender {}",
                key, getId(), this.getPartitionedRegion().getParallelGatewaySender());
          }
        }
        // In case of conflation and a race where bucket recovers
        // key-value from other bucket while put has come to this bucket.
        // if (object != null) {
        // ParallelGatewaySenderQueue queue =
        // (ParallelGatewaySenderQueue)getPartitionedRegion()
        // .getParallelGatewaySender().getQueues().toArray(new
        // RegionQueue[1])[0];
        // //queue.addToPeekedKeys(key);
        // }
        this.eventSeqNumDeque.remove(key);
      }
      return object; // OFFHEAP: ok since callers are careful to do destroys on
                     // region queue after finished with peeked object.
    } finally {
      getInitializationLock().readLock().unlock();
    }
  }

  protected void addToEventQueue(Object key, boolean didPut, EntryEventImpl event) {
    if (didPut) {
      if (this.initialized) {
        this.eventSeqNumDeque.addLast(key);
        updateLargestQueuedKey((Long) key);
      }
      if (logger.isDebugEnabled()) {
        if (event != null) {
          logger.debug("Put successfully in the queue : {} was initialized: {}",
              event.getRawNewValue(), this.initialized);
        }
      }
    }
    if (this.getBucketAdvisor().isPrimary()) {
      incQueueSize(1);
    } else {
      incSecondaryQueueSize(1);
    }
  }

  public void pushKeyIntoQueue(Object key) {
    eventSeqNumDeque.addFirst(key);
  }

  private void updateLargestQueuedKey(Long key) {
    Atomics.setIfGreater(this.latestQueuedKey, key);
  }

  private void setLatestAcknowledgedKey(Long key) {
    this.latestAcknowledgedKey.set(key);
  }

  public long getLatestQueuedKey() {
    return this.latestQueuedKey.get();
  }

  public boolean waitUntilFlushed(long latestQueuedKey, long timeout, TimeUnit unit)
      throws InterruptedException {
    long then = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("BucketRegionQueue: waitUntilFlushed bucket=" + getId() + "; latestQueuedKey="
          + latestQueuedKey + "; timeout=" + timeout + "; unit=" + unit);
    }
    boolean result = false;
    // Wait until latestAcknowledgedKey > latestQueuedKey or the queue is empty
    if (this.initialized) {
      long nanosRemaining = unit.toNanos(timeout);
      long endTime = System.nanoTime() + nanosRemaining;
      while (nanosRemaining > 0) {
        try {
          if (latestAcknowledgedKey.get() > latestQueuedKey || isEmpty()) {
            result = true;
            break;
          }
        } catch (RegionDestroyedException e) {
          if (this.isBucketDestroyed()) {
            getCancelCriterion().checkCancelInProgress(e);
            throw new BucketMovedException(this.getFullPath());
          }
        }
        Thread.sleep(Math.min(TimeUnit.NANOSECONDS.toMillis(nanosRemaining) + 1, 100));
        nanosRemaining = endTime - System.nanoTime();
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("BucketRegionQueue: waitUntilFlushed completed bucket=" + getId() + "; duration="
          + (System.currentTimeMillis() - then) + "; result=" + result);
    }
    return result;
  }

  /**
   * It removes the first key from the queue.
   *
   * @return Returns the key for which value was destroyed.
   */
  public Object remove() throws ForceReattemptException {
    Object key = this.eventSeqNumDeque.removeFirst();
    if (key != null) {
      destroyKey(key);
    }
    return key;
  }

  /**
   * It removes the first key from the queue.
   *
   * @return Returns the value.
   */
  public Object take() throws InterruptedException, ForceReattemptException {
    throw new UnsupportedOperationException();
    // Currently has no callers.
    // To support this callers need to call release on the returned GatewaySenderEventImpl.
    // Object key = this.eventSeqNumQueue.remove();
    // Object object = null;
    // if (key != null) {
    // object = PartitionRegionHelper
    // .getLocalPrimaryData(getPartitionedRegion()).get(key);
    // /**
    // * TODO: For the time being this is same as peek. To do a batch peek we
    // * need to remove the head key. We will destroy the key once the event is
    // * delivered to the GatewayReceiver.
    // */
    // destroyKey(key);
    // }
    // return object;
  }

  /**
   * Overriding this method from AbstractBucketRegionQueue in order to remove the event from
   * eventSeqNumQueue if EntryNotFoundException is encountered during basicDestroy. This change is
   * done during selective merge from r41425 from gemfire701X_maint.
   */
  public void destroyKey(Object key) throws ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug(" destroying primary key {}", key);
    }
    @Released
    EntryEventImpl event = newDestroyEntryEvent(key, null);
    try {
      event.setEventId(new EventID(cache.getInternalDistributedSystem()));
      event.setRegion(this);
      basicDestroy(event, true, null);
      setLatestAcknowledgedKey((Long) key);
      checkReadiness();
    } catch (EntryNotFoundException enf) {
      if (getPartitionedRegion().isDestroyed()) {
        getPartitionedRegion().checkReadiness();
        if (isBucketDestroyed()) {
          throw new ForceReattemptException("Bucket moved",
              new RegionDestroyedException(
                  LocalizedStrings.PartitionedRegionDataStore_REGION_HAS_BEEN_DESTROYED
                      .toLocalizedString(),
                  getPartitionedRegion().getFullPath()));
        }
      }
      throw enf;
    } catch (RegionDestroyedException rde) {
      getPartitionedRegion().checkReadiness();
      if (isBucketDestroyed()) {
        throw new ForceReattemptException("Bucket moved while destroying key " + key, rde);
      }
    } finally {
      event.release();
    }

    this.notifyEntriesRemoved();
  }

  public EntryEventImpl newDestroyEntryEvent(Object key, Object aCallbackArgument) {
    return getPartitionedRegion().newDestroyEntryEvent(key, aCallbackArgument);
  }

  public boolean isReadyForPeek() {
    return !this.getPartitionedRegion().isDestroyed() && !this.isEmpty()
        && !this.eventSeqNumDeque.isEmpty() && getBucketAdvisor().isPrimary();
  }

}
