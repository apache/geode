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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.ByteComparator;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.BucketRegionQueueUnavailableException;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.concurrent.Atomics;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;

/**
 * 
 */
public class BucketRegionQueue extends AbstractBucketRegionQueue {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * The <code>Map</code> mapping the regionName->key to the queue key. This
   * index allows fast updating of entries in the queue for conflation. This is
   * necesaary for Colocated regions and if any of the regions use same key for
   * data.
   */
  private final Map indexes;

  /**
   * A transient queue to maintain the eventSeqNum of the events that are to be
   * sent to remote site. It is cleared when the queue is cleared.
   */
  private final BlockingQueue<Object> eventSeqNumQueue = new LinkedBlockingQueue<Object>();
  
  //private final BlockingQueue<EventID> eventSeqNumQueueWithEventId = new LinkedBlockingQueue<EventID>();

  private long lastKeyRecovered;

  /**
   * @param regionName 
   * @param attrs
   * @param parentRegion
   * @param cache
   * @param internalRegionArgs
   */
  public BucketRegionQueue(String regionName, RegionAttributes attrs,
      LocalRegion parentRegion, GemFireCacheImpl cache,
      InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
    this.keySet();
    indexes = new ConcurrentHashMap<Object, Long>();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.internal.cache.BucketRegion#initialize(java.io.InputStream
   * ,
   * com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
   * , com.gemstone.gemfire.internal.cache.InternalRegionArguments)
   */
  @Override
  protected void initialize(InputStream snapshotInputStream,
      InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs) throws TimeoutException,
      IOException, ClassNotFoundException {

    super.initialize(snapshotInputStream, imageTarget, internalRegionArgs);

    //take initialization writeLock inside the method after synchronizing on tempQueue
    loadEventsFromTempQueue();
    
    getInitializationLock().writeLock().lock();
    try {
      if (!this.keySet().isEmpty()) {
        if (getPartitionedRegion().getColocatedWith() == null) {
          List<EventID> keys = new ArrayList<EventID>(this.keySet());
          Collections.sort(keys, new Comparator<EventID>() {
            @Override
            public int compare(EventID o1, EventID o2) {
              int compareMem = new ByteComparator().compare(
                  o1.getMembershipID(), o2.getMembershipID());
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
                  return o1.getSequenceID() < o2.getSequenceID() ? -1 : o1
                      .getSequenceID() == o2.getSequenceID() ? 0 : 1;
                }
              }
            }
          });
          for (EventID eventID : keys) {
            eventSeqNumQueue.add(eventID);
          }
        } else {
          TreeSet<Long> sortedKeys = new TreeSet<Long>(this.keySet());
          //although the empty check for this.keySet() is done above, 
          //do the same for sortedKeys as well because the keySet() might have become 
          //empty since the above check was made (keys might have been destroyed through BatchRemoval)
          //fix for #49679 NoSuchElementException thrown from BucketRegionQueue.initialize
          if (!sortedKeys.isEmpty()) {
            for (Long key : sortedKeys) {
              eventSeqNumQueue.add(key);
            }
            lastKeyRecovered = sortedKeys.last();
            if (this.getEventSeqNum() != null) {
              Atomics.setIfGreater(getEventSeqNum(), lastKeyRecovered);
            }
          }
        }

        if (logger.isDebugEnabled()) {
          logger.debug("For bucket {} ,total keys recovered are : {} last key recovered is : {} and the seqNo is ",
              getId(), eventSeqNumQueue.size(), lastKeyRecovered, getEventSeqNum());
        }
      }
      this.initialized = true;
      //Now, the bucket is initialized. Destroy the failedBatchRemovalKeys.
      destroyFailedBatchRemovalMessageKeys();
    }
    finally {
      notifyEventProcessor();
      getInitializationLock().writeLock().unlock();
    }
  }
  
  /**
   * When the GII was going, some of BatchRemoval messages (especially for events destroyed due to conflation) 
   * might have failed since the event might not be found in the BucketRegionQueue. 
   * Those messages are added to failedBatchRemovalMessageKeys map in ParallelQueueRemovalMessage.
   * Events found in the map need to be destroyed once GII is done. Fix for #47431.
   * This method is invoked deliberately after this.initialized is set to true to fix defect #50316.
   */
  private void destroyFailedBatchRemovalMessageKeys() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    Iterator<Object> itr = getFailedBatchRemovalMessageKeys().iterator();
    while (itr.hasNext()) {
      //at this point, failedBatchRemovalMessageKeys contains messages failed during bucket 
      //initialization only. Iterate over failedBatchRemovalMessageKeys and clear it completely.
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
    int batchSize = this.getPartitionedRegion().getParallelGatewaySender()
        .getBatchSize();
    Iterator<Object> itr = eventSeqNumQueue.iterator();
    markEventsAsDuplicate(batchSize, itr);
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
    this.eventSeqNumQueue.clear();
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
    this.eventSeqNumQueue.clear();
    return result.get();
  }
  

  @Override
  public void forceSerialized(EntryEventImpl event) {
    // NOOP since we want the value in the region queue to stay in object form.
  }

  protected void clearQueues(){
    getInitializationLock().writeLock().lock();
    try {
      this.indexes.clear();
      this.eventSeqNumQueue.clear();
    }
    finally {
      getInitializationLock().writeLock().unlock();
    }
  }
  
  @Override
  protected boolean virtualPut(EntryEventImpl event, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      long lastModified, boolean overwriteDestroyed) throws TimeoutException,
      CacheWriterException {
    boolean success = super.virtualPut(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);

    if (success) {
      GatewaySenderEventImpl.release(event.getRawOldValue());

      if (getPartitionedRegion().getColocatedWith() == null) {
        return success;
      }

      if (getPartitionedRegion().isConflationEnabled() && this.getBucketAdvisor().isPrimary()) {
        Object object = event.getNewValue();
        Long key = (Long)event.getKey();
        if (object instanceof Conflatable) {
          if (logger.isDebugEnabled()) {
            logger.debug("Key :{} , Object : {} is conflatable", key, object);
          }
          // TODO: TO optimize by destroying on primary and secondary separately
          // in case of conflation
          conflateOldEntry((Conflatable)object, key);
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Object : {} is not conflatable", object);
          }
        }
      }
    }
    return success;
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
      Map latestIndexesForRegion = (Map)this.indexes.get(rName);
      if (latestIndexesForRegion == null) {
        latestIndexesForRegion = new ConcurrentHashMap();
        this.indexes.put(rName, latestIndexesForRegion);
      }
      Long previousTailKey = (Long)latestIndexesForRegion.put(keyToConflate,
          tailKey);
      if (previousTailKey != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Conflating {} at queue index={} and previousTailKey: ", this, object, tailKey, previousTailKey);
        }
        AbstractGatewaySenderEventProcessor ep = region.getParallelGatewaySender().getEventProcessor();
        if (ep == null) return;
        ConcurrentParallelGatewaySenderQueue queue = (ConcurrentParallelGatewaySenderQueue)ep.getQueue();
        // Give the actual conflation work to another thread.
        // ParallelGatewaySenderQueue takes care of maintaining a thread pool.
        queue.conflateEvent(conflatableObject, getId(), previousTailKey);
      } else {
        region.getParallelGatewaySender().getStatistics()
            .incConflationIndexesMapSize();
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
      Conflatable object = (Conflatable)o;
      if (object.shouldBeConflated()) {
        // Otherwise, remove the index from the indexes map.
        String rName = object.getRegionToConflate();
        Object key = object.getKeyToConflate();
        Map latestIndexesForRegion = (Map)this.indexes.get(rName);
        if (latestIndexesForRegion != null) {
          // Remove the index.
          Long index = (Long)latestIndexesForRegion.remove(key);
          if (index != null) {
            this.getPartitionedRegion().getParallelGatewaySender()
                .getStatistics().decConflationIndexesMapSize();
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Removed index {} for {}", this, index, object);
            }
          }
        }
      }
    }
  }

  @Override
  protected void basicDestroy(final EntryEventImpl event,
      final boolean cacheWrite, Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {
    if (getPartitionedRegion().isConflationEnabled()) {
      removeIndex((Long)event.getKey());
    }
    super.basicDestroy(event, cacheWrite, expectedOldValue);

    GatewaySenderEventImpl.release(event.getRawOldValue());
    // Primary buckets should already remove the key while peeking
    if (!this.getBucketAdvisor().isPrimary()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" removing the key {} from eventSeqNumQueue", event.getKey());
      }
      this.eventSeqNumQueue.remove(event.getKey());
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
    	object = ((CachedDeserializable)object).getDeserializedValue(this, this.getRegionEntry(k));
      }
    } catch (EntryNotFoundException ok) {
      // just return null;
    }
    if (object == Token.TOMBSTONE) {
      object = null;
    }
    return object;  // OFFHEAP: ok since callers are careful to do destroys on region queue after finished with peeked object.
  }

  public Object peek() {
    Object key = null;
    Object object = null;
    //doing peek in initializationLock because during region destroy, the clearQueues 
    //clears the eventSeqNumQueue and can cause data inconsistency (defect #48984) 
    getInitializationLock().readLock().lock();
    try {
      if (this.getPartitionedRegion().isDestroyed()) {
        throw new BucketRegionQueueUnavailableException();
      }
      key = this.eventSeqNumQueue.peek();
      if (key != null) {
        object = optimalGet(key);
        if (object == null && !this.getPartitionedRegion().isConflationEnabled()) {
          if (logger.isDebugEnabled()) {
            logger.debug("The value against key {} in the bucket region queue with id {} is NULL for the GatewaySender {}",
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
        this.eventSeqNumQueue.remove(key);
      }
      return object; // OFFHEAP: ok since callers are careful to do destroys on
                     // region queue after finished with peeked object.
    }
    finally {
      getInitializationLock().readLock().unlock();
    }
  }

  protected void addToEventQueue(Object key, boolean didPut, EntryEventImpl event) {
    if (didPut) {
      if (this.initialized) {
        this.eventSeqNumQueue.add(key);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Put successfully in the queue : {} was initialized: {}", event.getRawNewValue(), this.initialized);
      }
    }
    if (this.getBucketAdvisor().isPrimary()) {
      incQueueSize(1);
    }
  }
  
  /**
   * It removes the first key from the queue.
   * 
   * @return Returns the key for which value was destroyed.
   * @throws ForceReattemptException
   */
  public Object remove() throws ForceReattemptException {
    Object key = this.eventSeqNumQueue.remove();  
    if (key != null) {
      destroyKey(key);
    }
    return key;
  }

  /**
   * It removes the first key from the queue.
   * 
   * @return Returns the value.
   * @throws InterruptedException
   * @throws ForceReattemptException
   */
  public Object take() throws InterruptedException, ForceReattemptException {
    throw new UnsupportedOperationException();
    // Currently has no callers.
    // To support this callers need to call release on the returned GatewaySenderEventImpl.
//     Object key = this.eventSeqNumQueue.remove();
//     Object object = null;
//     if (key != null) {
//       object = PartitionRegionHelper
//           .getLocalPrimaryData(getPartitionedRegion()).get(key);
//       /**
//        * TODO: For the time being this is same as peek. To do a batch peek we
//        * need to remove the head key. We will destroy the key once the event is
//        * delivered to the GatewayReceiver.
//        */
//       destroyKey(key);
//     }
//     return object;
  }
  
  /**
   * Overriding this method from AbstractBucketRegionQueue in order to remove
   * the event from eventSeqNumQueue if EntryNotFoundException is encountered 
   * during basicDestroy. 
   * This change is done during selective merge from r41425 from gemfire701X_maint.  
   */
  public void destroyKey(Object key) throws ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug(" destroying primary key {}", key);
    }
	@Released EntryEventImpl event = getPartitionedRegion().newDestroyEntryEvent(key,
	  null);
	try {
	  event.setEventId(new EventID(cache.getSystem()));
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

  public boolean isReadyForPeek() {
	return !this.getPartitionedRegion().isDestroyed() && !this.isEmpty() && !this.eventSeqNumQueue.isEmpty()
        && getBucketAdvisor().isPrimary();
  }

}
