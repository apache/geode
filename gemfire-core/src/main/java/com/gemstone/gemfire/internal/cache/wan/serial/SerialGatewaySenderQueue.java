/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 * 
 */
public class SerialGatewaySenderQueue implements RegionQueue {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * The key into the <code>Region</code> used when taking entries from the
   * queue. This value is either set when the queue is instantiated or read from
   * the <code>Region</code> in the case where this queue takes over where a
   * previous one left off.
   */
  private long headKey = -1;

  /**
   * The key into the <code>Region</code> used when putting entries onto the
   * queue. This value is either set when the queue is instantiated or read from
   * the <code>Region</code> in the case where this queue takes over where a
   * previous one left off.
   */
  private final AtomicLong tailKey = new AtomicLong();

  /**
   * The current key used to do put into the region. Once put is complete, then
   * the {@link #tailKey} is reconciled with this value.
   */
  private long currentKey;
  
  private final Deque<Long> peekedIds = new LinkedBlockingDeque<Long>();

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
  private String diskStoreName;

  /**
   * The maximum number of entries in a batch.
   */
  private int batchSize;

  /**
   * The maximum amount of memory (MB) to allow in the queue before overflowing
   * entries to disk
   */
  private int maximumQueueMemory;
  
  /**
   * Whether conflation is enabled for this queue.
   */
  private boolean enableConflation;

  /**
   * Whether persistence is enabled for this queue.
   */
  private boolean enablePersistence;
  
  /**
   * Whether write to disk is synchronous.
   */
  private boolean isDiskSynchronous;
  

  /**
   * The <code>Map</code> mapping the regionName->key to the queue key. This
   * index allows fast updating of entries in the queue for conflation.
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
  private static final boolean NO_ACK = Boolean
      .getBoolean("gemfire.gateway-queue-no-ack");
  
  private volatile long lastDispatchedKey = -1;
  
  private volatile long lastDestroyedKey = -1;
  
  public static final int DEFAULT_MESSAGE_SYNC_INTERVAL = 1;

  private static volatile int messageSyncInterval = DEFAULT_MESSAGE_SYNC_INTERVAL;
  
  private BatchRemovalThread removalThread = null;

  private final boolean keyPutNoSync;
  private final int maxPendingPuts;
  private final PriorityQueue<Long> pendingPuts;
  
  private AbstractGatewaySender sender  = null;

  public SerialGatewaySenderQueue(AbstractGatewaySender abstractSender,
      String regionName, CacheListener listener) {
    // The queue starts out with headKey and tailKey equal to -1 to force
    // them to be initialized from the region.
    this.regionName = regionName;
    this.headKey = -1;
    this.tailKey.set(-1);
    this.currentKey = -1;
    this.indexes = new HashMap<String, Map<Object, Long>>();
    this.enableConflation = abstractSender.isBatchConflationEnabled();
    this.diskStoreName = abstractSender.getDiskStoreName();
    this.batchSize = abstractSender.getBatchSize();
    this.enablePersistence = abstractSender.isPersistenceEnabled();
    if (this.enablePersistence) {
      this.isDiskSynchronous = abstractSender.isDiskSynchronous();
    } else {
      this.isDiskSynchronous = false;
    }
    if (Boolean.getBoolean("gemfire.gateway-queue-sync")) {
      this.keyPutNoSync = false;
      this.maxPendingPuts = 0;
      this.pendingPuts = null;
    }
    else {
      this.keyPutNoSync = true;
      this.maxPendingPuts = Math.max(this.batchSize, 100);
      this.pendingPuts = new PriorityQueue<Long>(this.maxPendingPuts + 5);
    }
    this.maximumQueueMemory = abstractSender.getMaximumMemeoryPerDispatcherQueue();
    this.stats = abstractSender.getStatistics();
    initializeRegion(abstractSender, listener);
    // Increment queue size. Fix for bug 51988.
    this.stats.incQueueSize(this.region.size());
    this.removalThread = new BatchRemovalThread((GemFireCacheImpl)abstractSender.getCache());
    this.removalThread.start();
    this.sender = abstractSender;
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Contains {} elements", this, size());
    }
    
    
  }

  public Region<Long, AsyncEvent> getRegion() {
    return this.region;
  }

  public void destroy() {
    getRegion().localDestroyRegion();
  }

  public synchronized void put(Object event) throws CacheException {
    GatewaySenderEventImpl eventImpl = (GatewaySenderEventImpl)event;
    final Region r = eventImpl.getRegion();
    final boolean isPDXRegion = (r instanceof DistributedRegion && r.getName()
        .equals(PeerTypeRegistration.REGION_NAME));
    final boolean isWbcl = this.regionName
        .startsWith(AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX);
    if (!(isPDXRegion && isWbcl)) {
      // TODO: Kishor : after merging this change. AsyncEventQueue test failed
      // with data inconsistency. As of now going ahead with sync putandGetKey.
      // Need to work on this during cedar
//      if (this.keyPutNoSync) {
//        putAndGetKeyNoSync(event);
//      }
//      else {
//        synchronized (this) {
          putAndGetKey(event);
        //}
      //}
    }
  }

  private long putAndGetKey(Object object) throws CacheException {
    // Get the tail key
    Long key = Long.valueOf(getTailKey());
    // Put the object into the region at that key
    this.region.put(key, (AsyncEvent)object);

    // Increment the tail key
    // It is important that we increment the tail
    // key after putting in the region, this is the
    // signal that a new object is available.
    incrementTailKey();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Inserted {} -> {}",this, key, object);
    }
    if (object instanceof Conflatable) {
      removeOldEntry((Conflatable)object, key);
    }
    return key.longValue();
  }

  private long putAndGetKeyNoSync(Object object) throws CacheException {
    // don't sync on whole put; callers will do the puts in parallel but
    // will wait later for previous tailKey put to complete after its own
    // put is done

    Long key;
    synchronized (this) {
      initializeKeys();
      // Get and increment the current key
      // Go for full sync in case of wrapover
      long ckey = this.currentKey;
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Determined current key: {}", this, ckey);
      }
      key = Long.valueOf(ckey);
      this.currentKey = inc(ckey);
    }

    try {
      // Put the object into the region at that key
      this.region.put(key, (AsyncEvent)object);

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Inserted {} -> {}", this, key, object);
      }
    } finally {

      final Object sync = this.pendingPuts;
      synchronized (sync) {
        // Increment the tail key
        // It is important that we increment the tail
        // key after putting in the region, this is the
        // signal that a new object is available.

        while (true) {
          if (key.longValue() == this.tailKey.get()) {
            // this is the next thread, so increment tail and signal all other
            // waiting threads if required
            incrementTailKey();
            // check pendingPuts
            boolean notifyWaiters = false;
            if (this.pendingPuts.size() > 0) {
              Iterator<Long> itr = this.pendingPuts.iterator();
              while (itr.hasNext()) {
                Long k = itr.next();
                if (k.longValue() == this.tailKey.get()) {
                  incrementTailKey();
                  // removed something from pending queue, so notify any waiters
                  if (!notifyWaiters) {
                    notifyWaiters =
                        (this.pendingPuts.size() >= this.maxPendingPuts);
                  }
                  itr.remove();
                }
                else {
                  break;
                }
              }
            }
            if (notifyWaiters) {
              sync.notifyAll();
            }
            break;
          }
          else if (this.pendingPuts.size() < this.maxPendingPuts) {
            this.pendingPuts.add(key);
            break;
          }
          else {
            // wait for the queue size to go down
            boolean interrupted = Thread.interrupted();
            Throwable t = null;
            try {
              sync.wait(5);
            } catch (InterruptedException ie) {
              t = ie;
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
              ((LocalRegion)this.region).getCancelCriterion()
                  .checkCancelInProgress(t);
            }
          }
        }
      }
    }

    if (object instanceof Conflatable) {
      removeOldEntry((Conflatable)object, key);
    }

    return key.longValue();
  }
  
  public synchronized AsyncEvent take() throws CacheException {
    // Unsupported since we have no callers.
    // If we do want to support it then each caller needs
    // to call freeOffHeapResources and the returned GatewaySenderEventImpl
    throw new UnsupportedOperationException();
//     resetLastPeeked();
//     AsyncEvent object = peekAhead();
//     // If it is not null, destroy it and increment the head key
//     if (object != null) {
//       Long key = this.peekedIds.remove();
//       if (logger.isTraceEnabled()) {
//         logger.trace("{}: Retrieved {} -> {}",this, key, object);
//       }
//       // Remove the entry at that key with a callback arg signifying it is
//       // a WAN queue so that AbstractRegionEntry.destroy can get the value
//       // even if it has been evicted to disk. In the normal case, the
//       // AbstractRegionEntry.destroy only gets the value in the VM.
//       this.region.destroy(key, RegionQueue.WAN_QUEUE_TOKEN);
//       updateHeadKey(key.longValue());

//       if (logger.isTraceEnabled()) {
//         logger.trace("{}: Destroyed {} -> {}", this, key, object);
//       }
//     }
//     return object;
  }

  public List<AsyncEvent> take(int batchSize) throws CacheException {
    // This method has no callers.
    // If we do want to support it then the callers
    // need to call freeOffHeapResources on each returned GatewaySenderEventImpl
    throw new UnsupportedOperationException();
//     List<AsyncEvent> batch = new ArrayList<AsyncEvent>(
//         batchSize * 2);
//     for (int i = 0; i < batchSize; i++) {
//       AsyncEvent obj = take();
//       if (obj != null) {
//         batch.add(obj);
//       } else {
//         break;
//       }
//     }
//     if (logger.isTraceEnabled()) {
//       logger.trace("{}: Took a batch of {} entries", this, batch.size());
//     }
//     return batch;
  }

  /**
   * This method removes the last entry. However, it will only let the user
   * remove entries that they have peeked. If the entry was not peeked, this
   * method will silently return.
   */
  public synchronized void remove() throws CacheException {
    if (this.peekedIds.isEmpty()) {
      return;
    }
    Long key = this.peekedIds.remove();
    try {
      // Increment the head key
      updateHeadKey(key.longValue());
      removeIndex(key);
      // Remove the entry at that key with a callback arg signifying it is
      // a WAN queue so that AbstractRegionEntry.destroy can get the value
      // even if it has been evicted to disk. In the normal case, the
      // AbstractRegionEntry.destroy only gets the value in the VM.
      this.region.localDestroy(key, WAN_QUEUE_TOKEN);
      this.stats.decQueueSize();
      
    } catch (EntryNotFoundException ok) {
      // this is acceptable because the conflation can remove entries
      // out from underneath us.
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Did not destroy entry at {} it was not there. It should have been removed by conflation.", this, key);
      }
    }

    boolean wasEmpty = this.lastDispatchedKey == this.lastDestroyedKey;
    this.lastDispatchedKey = key;
    if (wasEmpty) {
      this.notify();
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Destroyed entry at key {} setting the lastDispatched Key to {}. The last destroyed entry was {}",
          this, key, this.lastDispatchedKey, this.lastDestroyedKey);
    }
  }

  /**
   * This method removes batchSize entries from the queue. It will only remove
   * entries that were previously peeked.
   * 
   * @param size
   *          the number of entries to remove
   */
  public void remove(int size) throws CacheException {
    for (int i = 0; i < size; i++) {
      remove();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Removed a batch of {} entries", this, size);
    }
  }
  
  public void remove(Object object)
  {
    remove();
  }

  public Object peek() throws CacheException {
    //resetLastPeeked();
    Object object = peekAhead();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Peeked {} -> {}", this, peekedIds, object);
    }

    return object;
    // OFFHEAP returned object only used to see if queue is empty
    // so no need to worry about off-heap refCount.
  }

  public List<AsyncEvent> peek(int size) throws CacheException {
    return peek(size, -1);
  }

  public List<AsyncEvent> peek(int size, int timeToWait)
      throws CacheException {
    final boolean isTraceEnabled = logger.isTraceEnabled();
    
    long start = System.currentTimeMillis();
    long end = start + timeToWait;
    if (isTraceEnabled) {
      logger.trace("{}: Peek start time={} end time={} time to wait={}", this, start, end, timeToWait);
    }
    List<AsyncEvent> batch = new ArrayList<AsyncEvent>(size * 2); // why
                                                                                // *2?
    //resetLastPeeked();
    while (batch.size() < size) {
      AsyncEvent object = peekAhead();
      if (object != null && object instanceof GatewaySenderEventImpl) {
        GatewaySenderEventImpl copy = ((GatewaySenderEventImpl)object).makeHeapCopyIfOffHeap();
        if (copy == null) {
          continue;
        }
        object = copy;
      }
      // Conflate here
      if (object != null) {
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
    if (isTraceEnabled) {
      logger.trace("{}: Peeked a batch of {} entries", this, batch.size());
    }
    return batch;
    // OFFHEAP: all returned AsyncEvent end up being removed from queue after the batch is sent
    // so no need to worry about off-heap refCount.
  }
  
  @Override
  public String toString() {
    return "SerialGatewaySender queue :" + this.regionName;
  }

  public int size() {
    int size = ((LocalRegion) this.region).entryCount();
    return size + this.sender.getTmpQueuedEventSize();
  }

  @SuppressWarnings("rawtypes")
  public void addCacheListener(CacheListener listener) {
    AttributesMutator mutator = this.region.getAttributesMutator();
    mutator.addCacheListener(listener);
  }

  @SuppressWarnings("rawtypes")
  public void removeCacheListener() {
    AttributesMutator mutator = this.region.getAttributesMutator();
    CacheListener[] listeners = this.region.getAttributes().getCacheListeners();
    for(int i=0; i < listeners.length; i++){
      if(listeners[i] instanceof SerialSecondaryGatewayListener){
        mutator.removeCacheListener(listeners[i]);
        break;
      }
    }
  }

  private boolean removeOldEntry(Conflatable object, Long tailKey)
      throws CacheException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    boolean keepOldEntry = true;

    // Determine whether conflation is enabled for this queue and object
    // Conflation is enabled iff:
    // - this queue has conflation enabled
    // - the object can be conflated
    if (this.enableConflation && object.shouldBeConflated()) {
      if (isDebugEnabled) {
        logger.debug("{}: Conflating {} at queue index={} queue size={} head={} tail={}",
            this, object, tailKey, size(), this.headKey, tailKey);
      }

      // Determine whether this region / key combination is already indexed.
      // If so, it is already in the queue. Update the value in the queue and
      // set the shouldAddToQueue flag accordingly.
      String rName = object.getRegionToConflate();
      Object key = object.getKeyToConflate();
      Long previousIndex;

      synchronized (this) {
    	Map<Object, Long> latestIndexesForRegion = this.indexes.get(rName);
    	if (latestIndexesForRegion == null) {
    	  latestIndexesForRegion = new HashMap<Object, Long>();
    	  this.indexes.put(rName, latestIndexesForRegion);
    	}

    	previousIndex = latestIndexesForRegion.put(key, tailKey);
      }
      
      if (isDebugEnabled) {
        logger.debug("{}: Adding index key={}->index={} for {} head={} tail={}",
            this, key, tailKey, object, this.headKey, tailKey);
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
          logger.debug("{}: Indexes contains index={} for key={} head={} tail={} and it can be used.",
              this, previousIndex, key, this.headKey, tailKey);
        }
        keepOldEntry = false;
      } else {
        if (isDebugEnabled) {
          logger.debug("{}: No old entry for key={} head={} tail={} not removing old entry.",
              this, key, this.headKey, tailKey);
        }
        this.stats.incConflationIndexesMapSize();
        keepOldEntry = true;
      }

      // Replace the object's value into the queue if necessary
      if (!keepOldEntry) {
        Conflatable previous = (Conflatable)this.region.remove(previousIndex);
        this.stats.decQueueSize(1); 
        if (isDebugEnabled) {
          logger.debug("{}: Previous conflatable at key={} head={} tail={}: {}",
              this, previousIndex, this.headKey, tailKey, previous);
          logger.debug("{}: Current conflatable at key={} head={} tail={}: {}",
              this, tailKey, this.headKey, tailKey, object);
          if (previous != null) {
            logger.debug("{}: Removed {} and added {} for key={} head={} tail={} in queue for region={} old event={}",
                this, previous.getValueToConflate(), object.getValueToConflate(),
                key, this.headKey, tailKey, rName, previous);
          }
        }
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("{}: Not conflating {} queue size: {} head={} tail={}", this, object, size(), this.headKey, tailKey);
      }
    }
    return keepOldEntry;
  }

  /**
   * Does a get that gets the value without fault values in from disk.
   */
  private AsyncEvent optimalGet(Long k) {
	// Get the object at that key (to remove the index).
	LocalRegion lr = (LocalRegion)this.region;
	Object o = null;
	try {
		o = lr.getValueInVMOrDiskWithoutFaultIn(k); 
		if (o != null && o instanceof CachedDeserializable) { 
			o = ((CachedDeserializable)o).getDeserializedValue(lr, lr.getRegionEntry(k));
		}	
	} catch (EntryNotFoundException ok) {
		// just return null;
	}
	// bug #46023 do not return a destroyed entry marker
	if (o == Token.TOMBSTONE) {
		o = null;
	}
	return (AsyncEvent)o;
  }

  // No need to synchronize because it is called from a synchronized method
  private void removeIndex(Long qkey) {
    // Determine whether conflation is enabled for this queue and object
    if (this.enableConflation) {
      // only call get after checking enableConflation for bug 40508
      Object o = optimalGet(qkey);
      if (o instanceof Conflatable) {
        Conflatable object = (Conflatable)o;
        if (object.shouldBeConflated()) {
          // Otherwise, remove the index from the indexes map.
          String rName = object.getRegionToConflate();
          Object key = object.getKeyToConflate();

          Map<Object, Long> latestIndexesForRegion = this.indexes.get(rName);
          if (latestIndexesForRegion != null) {
            // Remove the index.
            Long index = latestIndexesForRegion.remove(key);
            // Decrement the size if something was removed. This check is for
            // the case where failover has occurred and the entry was not put
            // into the map initially.
            if (index != null) {
              this.stats.decConflationIndexesMapSize();
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
   * returns true if key a is before key b. This test handles keys that have
   * wrapped around
   * 
   * @param a
   * @param b
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

  /**
   * Clear the list of peeked keys. The next peek will start again at the head
   * key.
   * 
   */
  public void resetLastPeeked() {
    this.peekedIds.clear();
  }

  /**
   * Finds the next object after the last key peeked
   * 
   * @throws CacheException
   */
  private AsyncEvent peekAhead() throws CacheException {
    AsyncEvent object = null;
    long currentKey = -1;
    if (this.peekedIds.isEmpty()) {
    	currentKey = getHeadKey(); 
    } else {
    	Long lastPeek = this.peekedIds.peekLast();
    	if (lastPeek == null) {
    		return null;
    	}
    	currentKey = lastPeek.longValue() + 1;
    }
    
    
    // It's important here that we check where the current key
    // is in relation to the tail key before we check to see if the
    // object exists. The reason is that the tail key is basically
    // the synchronization between this thread and the putter thread.
    // The tail key will not be incremented until the object is put in the
    // region
    // If we check for the object, and then check the tail key, we could
    // skip objects.

    // @todo don't do a get which updates the lru, instead just get the value
    // w/o modifying the LRU.
    // Note: getting the serialized form here (if it has overflowed to disk)
    // does not save anything since GatewayBatchOp needs to GatewayEventImpl
    // in object form.
    while (before(currentKey, getTailKey())
    // use optimalGet here to fix bug 40654
        && (object = optimalGet(Long.valueOf(currentKey))) == null) {
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Trying head key + offset: {}", this, currentKey);
      }
      currentKey = inc(currentKey);
      if (this.stats != null) {
        this.stats.incEventsNotQueuedConflated();
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Peeked {}->{}", this, currentKey, object);
    }
    if (object != null) {
      this.peekedIds.add(Long.valueOf(currentKey));
    }
    return object;
  }

  /**
   * Returns the value of the tail key. The tail key points to an empty where
   * the next queue entry will be stored.
   * 
   * @return the value of the tail key
   * @throws CacheException
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

    tlKey = this.tailKey.get();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Determined tail key: {}", this, tlKey);
    }
    return tlKey;
  }

  /**
   * Increments the value of the tail key by one.
   * 
   * @throws CacheException
   */
  private void incrementTailKey() throws CacheException {
    this.tailKey.set(inc(this.tailKey.get()));
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Incremented TAIL_KEY for region {} to {}", this, this.region.getName(), this.tailKey);
    }
  }

  /**
   * If the keys are not yet initialized, initialize them from the region .
   * 
   * TODO - We could initialize the indexes maps at the time here. However, that
   * would require iterating over the values of the region rather than the keys,
   * which could be much more expensive if the region has overflowed to disk.
   * 
   * We do iterate over the values of the region in SerialGatewaySender at the
   * time of failover. see SerialGatewaySender.handleFailover. So there's a
   * possibility we can consolidate that code with this method and iterate over
   * the region once.
   * 
   * @throws CacheException
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

      Set<Long> keySet = this.region.keySet();
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
      if (smallestKeyGreaterThanHalfMax != -1
          && largestKeyLessThanHalfMax != -1
          && (smallestKeyGreaterThanHalfMax - largestKeyLessThanHalfMax) > MAXIMUM_KEY / 2) {
        this.headKey = smallestKeyGreaterThanHalfMax;
        this.tailKey.set(inc(largestKeyLessThanHalfMax));
        logger.info(LocalizedMessage.create(LocalizedStrings.SingleWriteSingleReadRegionQueue_0_DURING_FAILOVER_DETECTED_THAT_KEYS_HAVE_WRAPPED,
                  new Object[] { this, this.tailKey, Long.valueOf(this.headKey) }));
      } else {
        this.headKey = smallestKey == -1 ? 0 : smallestKey;
        this.tailKey.set(inc(largestKey));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initialized tail key to: {}, head key to: {}", this, this.tailKey, this.headKey);
      }
    }
  }

  /**
   * Returns the value of the head key. The head key points to the next entry to
   * be removed from the queue.
   * 
   * @return the value of the head key
   * @throws CacheException
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
    hKey = this.headKey;
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Determined head key: {}", this, hKey);
    }
    return hKey;
  }

  /**
   * Increments the value of the head key by one.
   * 
   * @throws CacheException
   */
  private void updateHeadKey(long destroyedKey) throws CacheException {
    this.headKey = inc(destroyedKey);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Incremented HEAD_KEY for region {} to {}", this, this.region.getName(), this.headKey);
    }
  }

  /**
   * Initializes the <code>Region</code> backing this queue. The
   * <code>Region</code>'s scope is DISTRIBUTED_NO_ACK and mirror type is
   * KEYS_VALUES and is set to overflow to disk based on the
   * <code>GatewayQueueAttributes</code>.
   * 
   * @param sender
   *          The GatewaySender <code>SerialGatewaySenderImpl</code>
   * @param listener
   *          The GemFire <code>CacheListener</code>. The
   *          <code>CacheListener</code> can be null.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void initializeRegion(AbstractGatewaySender sender,
      CacheListener listener) {
    final GemFireCacheImpl gemCache = (GemFireCacheImpl)sender.getCache();
    this.region = gemCache.getRegion(this.regionName);
    if (this.region == null) {
      AttributesFactory<Long, AsyncEvent> factory = new AttributesFactory<Long, AsyncEvent>();
      factory.setScope(NO_ACK ? Scope.DISTRIBUTED_NO_ACK
          : Scope.DISTRIBUTED_ACK);
      factory
          .setDataPolicy(this.enablePersistence ? DataPolicy.PERSISTENT_REPLICATE
              : DataPolicy.REPLICATE);
      if (logger.isDebugEnabled()) {
        logger.debug("The policy of region is {}",
            (this.enablePersistence ? DataPolicy.PERSISTENT_REPLICATE: DataPolicy.REPLICATE));
      }
      // Set listener if it is not null. The listener will be non-null
      // when the user of this queue is a secondary VM.
      if (listener != null) {
        factory.addCacheListener(listener);
      }
      // allow for no overflow directory
      EvictionAttributes ea = EvictionAttributes
          .createLIFOMemoryAttributes(this.maximumQueueMemory,
              EvictionAction.OVERFLOW_TO_DISK);
      
      factory.setEvictionAttributes(ea);
      factory.setConcurrencyChecksEnabled(false);

      
      factory.setDiskStoreName(this.diskStoreName);
      // TODO: Suranjan, can we do the following
      // In case of persistence write to disk sync and in case of eviction
      // write in async
      factory.setDiskSynchronous(this.isDiskSynchronous);
      
      // Create the region
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Attempting to create queue region: {}", this, this.regionName);
      }
      final RegionAttributes<Long, AsyncEvent> ra = factory.create();
      try {
        SerialGatewaySenderQueueMetaRegion meta = new SerialGatewaySenderQueueMetaRegion(
            this.regionName, ra, null, gemCache, sender);
        try {
          this.region = gemCache.createVMRegion(
              this.regionName,
              ra,
              new InternalRegionArguments().setInternalMetaRegion(meta)
                  .setDestroyLockFlag(true).setSnapshotInputStream(null)
                  .setImageTarget(null)
                  .setIsUsedForSerialGatewaySenderQueue(true)
                  .setSerialGatewaySender(sender));
          
        } catch (IOException veryUnLikely) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass()), veryUnLikely);
        } catch (ClassNotFoundException alsoUnlikely) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0,
                  this.getClass()), alsoUnlikely);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Created queue region: {}",this, this.region);
        }
      } catch (CacheException e) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.SingleWriteSingleReadRegionQueue_0_THE_QUEUE_REGION_NAMED_1_COULD_NOT_BE_CREATED,
                new Object[] { this, this.regionName }), e);
      }
    } else {
      throw new IllegalStateException("Queue region " + this.region.getFullPath() + " already exists.");
    }
  }

  public void cleanUp() {
    if (this.removalThread != null) {
      this.removalThread.shutdown();
    }
  }
  
  @Override
  public void close() {
    Region r = getRegion();
    if (r != null && !r.isDestroyed()) {
      try {
        r.close();
      } catch (RegionDestroyedException e) {
      }
    }
  }
  
  private class BatchRemovalThread extends Thread {
    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final GemFireCacheImpl cache;

    /**
     * Constructor : Creates and initializes the thread
     */
    public BatchRemovalThread(GemFireCacheImpl c) {
      this.setDaemon(true);
      this.cache = c;
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }
      if (cache.getCancelCriterion().cancelInProgress() != null) {
        return true;
      }
      return false;
    }

    @Override
    public void run() {
      InternalDistributedSystem ids = cache.getDistributedSystem();

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
                this.wait(messageSyncInterval * 1000);
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
              if (interrupted)
                Thread.currentThread().interrupt();
            }

            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread about to send the last Dispatched key {}", lastDispatchedKey);
            }
            
            long temp;
            synchronized (SerialGatewaySenderQueue.this) {
              temp = lastDispatchedKey;
              boolean wasEmpty = temp == lastDestroyedKey;
              while (lastDispatchedKey == lastDestroyedKey) {
                SerialGatewaySenderQueue.this.wait();
                temp = lastDispatchedKey;
              }
              if (wasEmpty) continue;
            }
            
            EntryEventImpl event = EntryEventImpl.create((LocalRegion)region,
                Operation.DESTROY, (lastDestroyedKey + 1) , null/* newValue */, null, false,
                cache.getMyId());
            event.disallowOffHeapValues();
            event.setTailKey(temp);
            
            BatchDestroyOperation op =  new BatchDestroyOperation(event);
            op.distribute();
            if (logger.isDebugEnabled()) {
              logger.debug("BatchRemovalThread completed destroy of keys from {} to {}", lastDestroyedKey, temp);
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
        logger.info(LocalizedMessage.create(LocalizedStrings.HARegionQueue_THE_QUEUEREMOVALTHREAD_IS_DONE));
      }
    }

    /**
     * shutdown this thread and the caller thread will join this thread
     */
    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
      boolean interrupted = Thread.interrupted();
      try {
        this.join(15 * 1000);
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (this.isAlive()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.HARegionQueue_QUEUEREMOVALTHREAD_IGNORED_CANCELLATION));
      }
    }
  }
  
  public static class SerialGatewaySenderQueueMetaRegion extends
      DistributedRegion {
    AbstractGatewaySender sender = null;
    protected SerialGatewaySenderQueueMetaRegion(String regionName,
        RegionAttributes attrs, LocalRegion parentRegion,
        GemFireCacheImpl cache, AbstractGatewaySender sender) {
      super(regionName, attrs, parentRegion, cache,
          new InternalRegionArguments().setDestroyLockFlag(true)
              .setRecreateFlag(false).setSnapshotInputStream(null)
              .setImageTarget(null).setIsUsedForSerialGatewaySenderQueue(true).setSerialGatewaySender(sender));
      this.sender = sender;
    }

    //Prevent this region from using concurrency checks
    @Override
    final public boolean supportsConcurrencyChecks() {
      return false;
    }

    @Override
    protected boolean isCopyOnRead() {
      return false;
    }

    // Prevent this region from participating in a TX, bug 38709
    @Override
    final public boolean isSecret() {
      return true;
    }

    // @override event tracker not needed for this type of region
    @Override
    public void createEventTracker() {
    }

    @Override
    final protected boolean shouldNotifyBridgeClients() {
      return false;
    }

    @Override
    final public boolean generateEventID() {
      return false;
    }
    
    @Override
    final public boolean isUsedForSerialGatewaySenderQueue() {
      return true;
    }

    @Override
    final public AbstractGatewaySender getSerialGatewaySender() {
      return sender;
    }
    @Override
    public void closeEntries() {
      OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
        @Override
        public void run() {
          SerialGatewaySenderQueueMetaRegion.super.closeEntries();
        }
      });
    }
    @Override
    public Set<VersionSource> clearEntries(final RegionVersionVector rvv) {
      final AtomicReference<Set<VersionSource>> result = new AtomicReference<Set<VersionSource>>();
      OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
        @Override
        public void run() {
          result.set(SerialGatewaySenderQueueMetaRegion.super.clearEntries(rvv));
        }
      });
      return result.get();
    }
    @Override
    protected void basicDestroy(final EntryEventImpl event,
        final boolean cacheWrite, Object expectedOldValue)
        throws EntryNotFoundException, CacheWriterException, TimeoutException {

      super.basicDestroy(event, cacheWrite, expectedOldValue);
      GatewaySenderEventImpl.release(event.getRawOldValue());
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
      }
      return success;
    }
  }
}
