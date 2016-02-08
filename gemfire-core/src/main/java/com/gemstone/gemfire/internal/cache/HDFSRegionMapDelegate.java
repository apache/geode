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
package com.gemstone.gemfire.internal.cache;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSEntriesSet;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSEntriesSet.HDFSIterator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSGatewayEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSParallelGatewaySenderQueue;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.LocalRegion.IteratorType;
import com.gemstone.gemfire.internal.cache.RegionMap.Attributes;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.lru.LRUEntry;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.concurrent.FutureResult;

/**
 * This class encapsulates all the functionality of HDFSRegionMap, so
 * that it can be provided to HDFSLRURegionMap. 
 * 
 * @author sbawaska
 */
public class HDFSRegionMapDelegate {

  private static final Logger logger = LogService.getLogger();

  private final BucketRegion owner;

  private ConcurrentParallelGatewaySenderQueue hdfsQueue;

  private final RegionMap backingRM;

  /** queue of dead iterators */
  private final ReferenceQueue<HDFSIterator> refs;
  
  private static final boolean DEBUG = Boolean.getBoolean("hdfsRegionMap.DEBUG");
  
  /**
   * used for serializing fetches from HDFS
   */
  private ConcurrentMap<Object, FutureResult> futures = new ConcurrentHashMap<Object, FutureResult>();

  public HDFSRegionMapDelegate(LocalRegion owner, Attributes attrs,
      InternalRegionArguments internalRegionArgs, RegionMap backingRM) {
    assert owner instanceof BucketRegion;
    this.owner = (BucketRegion) owner;
    this.backingRM = backingRM;
    refs = new ReferenceQueue<HDFSEntriesSet.HDFSIterator>();
  }

  public RegionEntry getEntry(Object key, EntryEventImpl event) {
    
    RegionEntry re = getEntry(key, event, true);
    // get from tx should put the entry back in map
    // it should be evicted once tx completes
    /**MergeGemXDHDFSToGFE txstate does not apply for this*/
    /* if (re != null && getTXState(event) != null) {
    if (re != null) {
      // put the region entry in backing CHM of AbstractRegionMap so that
      // it can be locked in basicPut/destroy
      RegionEntry oldRe = backingRM.putEntryIfAbsent(key, re);
      if (oldRe != null) {
        if (re instanceof OffHeapRegionEntry && !oldRe.equals(re)) {
          ((OffHeapRegionEntry)re).release();
        }
        return oldRe;
      }
      re.setMarkedForEviction();
      owner.updateSizeOnCreate(key,
          owner.calculateRegionEntryValueSize(re));
      ((AbstractRegionMap)backingRM).incEntryCount(1);
      ((AbstractRegionMap)backingRM).lruEntryCreate(re);
    }*/
    return re;
  }

  /*
  private TXStateInterface getTXState(EntryEventImpl event) {
    return event != null ? event.getTXState(this.owner) : this.owner
        .getTXState();
  }*/

  /**
   * 
   * @param key
   * @param event
   * @param forceOnHeap if true will return heap version of off-heap region entries
   */
  private RegionEntry getEntry(Object key, EntryEventImpl event, boolean forceOnHeap) {
    closeDeadIterators();
    
    RegionEntry re = backingRM.getEntryInVM(key);
    if (logger.isTraceEnabled() || DEBUG) {
      logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: Found the key in CHM: " + key
          + " ,value=" + (re == null? "null" : "[" + re._getValue() + " or (" + re.getValueAsToken() + ")]")));
    }
    if ((re == null || (re.isRemoved() && !re.isTombstone()))
        && owner.getBucketAdvisor().isPrimary() && allowReadFromHDFS()) {
      if (logger.isTraceEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: fetching from hdfs key:" + key));
      }
      try {
        this.owner.getPartitionedRegion().hdfsCalled(key);
        re = getEntryFromFuture(key);
        if (re != null) {
          return re;
        }

        assert this.owner.getPartitionedRegion().getDataPolicy()
            .withHDFS();
        byte[] k = EntryEventImpl.serialize(key);
      
        // for destroy ops we will retain the entry in the region map so
        // tombstones can be tracked
        //final boolean forceOnHeap = (event==null || !event.getOperation().isDestroy());
        
        // get from queue
        re = getFromHDFSQueue(key, k, forceOnHeap);
        if (re == null) {
          // get from HDFS
          re = getFromHDFS(key, k, forceOnHeap);
        }
        if (re != null && re.isTombstone()) {
          RegionVersionVector vector = this.owner.getVersionVector();
//          if (vector == null) {
//            this.owner.getLogWriterI18n().info(LocalizedStrings.DEBUG,
//            "found a tombstone in a region w/o a version vector: " + re + "; region: " + this.owner);
//          }
          if (vector == null
              || vector.isTombstoneTooOld(re.getVersionStamp().getMemberID(),
                                    re.getVersionStamp().getRegionVersion())) {
            re = null;
          }
        }
        if (logger.isTraceEnabled() || DEBUG) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: returning from hdfs re:" + re));
        }
      } catch (ForceReattemptException e) {
        throw new PrimaryBucketException(e.getLocalizedMessage(), e);
      } catch (IOException e) {
        throw new HDFSIOException("Error reading from HDFS", e);
      } finally {
        notifyFuture(key, re);
        // If we mark it here, the table scan may miss it causing updates/delete using table
        // scan to fail.
//        if (re != null) {
//          re.setMarkedForEviction();
//        }
        if(re != null && event != null && !re.isTombstone()) {
          if (logger.isTraceEnabled() || DEBUG) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: loaded from hdfs re:" + re));
          }
          BucketRegion br = (BucketRegion)owner;
          //CustomEvictionAttributes csAttr = br.getCustomEvictionAttributes();
          //if(csAttr!=null)
          event.setLoadedFromHDFS(true);
        }
      }
    }
    if(re!=null && re.isMarkedForEviction() && !re.isTombstone()) {
      if(event!=null) {
        event.setLoadedFromHDFS(true);
      }
    }

    return re;
  }

  /**
   * This method returns true if the RegionEntry should be read from HDFS.
   * fixes #49101 by not allowing reads from HDFS for persistent regions
   * that do not define an eviction criteria.
   * 
   * @return true if RegionEntry should be read from HDFS
   */
  private boolean allowReadFromHDFS() {
    if (!owner.getDataPolicy().withPersistence()
        || owner.getCustomEvictionAttributes() != null
        || isEvictionActionLocalDestroy()){
        /**MergeGemXDHDFSToGFE this is used for global index. Hence not required here*/ 
        //|| owner.isUsedForIndex()) {
      // when region does not have persistence, we have to read from HDFS (even
      // though there is no eviction criteria) for constraint checks
      return true;
    }
    return false;
  }

  private boolean isEvictionActionLocalDestroy() {
    PartitionedRegion pr = owner.getPartitionedRegion();
    if (pr.getEvictionAttributes() != null) {
      return pr.getEvictionAttributes().getAction() == EvictionAction.LOCAL_DESTROY;
    }
    return false;
  }

  protected RegionEntry getEntry(EntryEventImpl event) {
    RegionEntry re = getEntry(event.getKey(), event, false);
    if (re != null && event.isLoadedFromHDFS()) {
      // put the region entry in backing CHM of AbstractRegionMap so that
      // it can be locked in basicPut/destroy
      RegionEntry oldRe = backingRM.putEntryIfAbsent(event.getKey(), re);
      if (oldRe != null) {
        if (re instanceof OffHeapRegionEntry && !oldRe.equals(re)) {
          ((OffHeapRegionEntry) re).release();
        }
        return oldRe;
      }
      // since the entry is faulted in from HDFS, it must have
      // satisfied the eviction criteria in the past, so mark it for eviction
      re.setMarkedForEviction();

      owner.updateSizeOnCreate(event.getKey(), owner.calculateRegionEntryValueSize(re));
      ((AbstractRegionMap) backingRM).incEntryCount(1);
      ((AbstractRegionMap) backingRM).lruEntryCreate(re);
    }
    return re;
  }

  @SuppressWarnings("unchecked")
  public Collection<RegionEntry> regionEntries() {
    closeDeadIterators();
    if (!owner.getPartitionedRegion().includeHDFSResults()) {
      if (logger.isDebugEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Ignoring HDFS results for #regionEntries"));
      }
      return backingRM.regionEntriesInVM();
    }

    try {
      return createEntriesSet(IteratorType.ENTRIES);
    } catch (ForceReattemptException e) {
      throw new PrimaryBucketException(e.getLocalizedMessage(), e);
    }
  }
    
  public int size() {
    closeDeadIterators();
    if (!owner.getPartitionedRegion().includeHDFSResults()) {
      if (logger.isDebugEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Ignoring HDFS results for #size"));
      }
      return backingRM.sizeInVM();
    }

    try {
      return createEntriesSet(IteratorType.KEYS).size();
    } catch (ForceReattemptException e) {
      throw new PrimaryBucketException(e.getLocalizedMessage(), e);
    }
  }
    
  public boolean isEmpty() {
    closeDeadIterators();
    if (!owner.getPartitionedRegion().includeHDFSResults()) {
      if (logger.isDebugEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Ignoring HDFS results for #isEmpty"));
      }
      return backingRM.sizeInVM() == 0;
    }

    try {
      return createEntriesSet(IteratorType.KEYS).isEmpty();
    } catch (ForceReattemptException e) {
      throw new PrimaryBucketException(e.getLocalizedMessage(), e);
    }
  }
  
  private void notifyFuture(Object key, RegionEntry re) {
    FutureResult future = this.futures.remove(key);
    if (future != null) {
      future.set(re);
    }
  }

  private RegionEntry getEntryFromFuture(Object key) {
    FutureResult future = new FutureResult(this.owner.getCancelCriterion());
    FutureResult old = this.futures.putIfAbsent(key, future);
    if (old != null) {
      if (logger.isTraceEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: waiting for concurrent fetch to complete for key:" + key));
      }
      try {
        return (RegionEntry) old.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        this.owner.getCache().getCancelCriterion().checkCancelInProgress(null);
      }
    }
    return null;
  }

  private RegionEntry getFromHDFS(Object key, byte[] k, boolean forceOnHeap) throws IOException, ForceReattemptException {
    SortedHoplogPersistedEvent ev;
    try {
      ev = (SortedHoplogPersistedEvent) owner.getHoplogOrganizer().read(k);
    } catch (IOException e) {
      owner.checkForPrimary();
      throw e;
    }
    if (ev != null) {
      if (logger.isTraceEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: got from hdfs ev:" + ev));
      }
      return getEntryFromEvent(key, ev, forceOnHeap, false);
    }
    return null;
  }

  /**
   * set the versionTag on the newly faulted-in entry
   */
  private void setVersionTag(RegionEntry re, VersionTag versionTag) {
    if (owner.concurrencyChecksEnabled) {
      versionTag.setMemberID(
            owner.getVersionVector().getCanonicalId(versionTag.getMemberID()));
      VersionStamp versionedRe = (VersionStamp) re;
      versionedRe.setVersions(versionTag);
    }
  }

  private RegionEntry getFromHDFSQueue(Object key, byte[] k, boolean forceOnHeap) throws ForceReattemptException {
    ConcurrentParallelGatewaySenderQueue q = getHDFSQueue();
    if (q == null) return null;
    HDFSGatewayEventImpl hdfsGatewayEvent = (HDFSGatewayEventImpl) q.get(owner.getPartitionedRegion(), k, owner.getId());
    if (hdfsGatewayEvent != null) {
      if (logger.isTraceEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "HDFS: got from hdfs queue: " + hdfsGatewayEvent));
      }
      return getEntryFromEvent(key, hdfsGatewayEvent, forceOnHeap, false);
    }
    return null;
  }

  private ConcurrentParallelGatewaySenderQueue getHDFSQueue()
      throws ForceReattemptException {
    if (this.hdfsQueue == null) {
      String asyncQId = this.owner.getPartitionedRegion().getHDFSEventQueueName();
      final AsyncEventQueueImpl asyncQ =  (AsyncEventQueueImpl)this.owner.getCache().getAsyncEventQueue(asyncQId);
      final AbstractGatewaySender gatewaySender = (AbstractGatewaySender)asyncQ.getSender();
      AbstractGatewaySenderEventProcessor ep = gatewaySender.getEventProcessor();
      if (ep == null) return null;
      hdfsQueue = (ConcurrentParallelGatewaySenderQueue)ep.getQueue();
    }
    
    // Check whether the queue has become primary here.
    // There could be some time between bucket becoming a primary
    // and underlying queue becoming a primary, so isPrimaryWithWait()
    // waits for some time for the queue to become a primary on this member
    final HDFSBucketRegionQueue brq = hdfsQueue.getBucketRegionQueue(
        this.owner.getPartitionedRegion(), this.owner.getId());
    if (brq != null) {
      if (owner.getBucketAdvisor().isPrimary()
          && !brq.getBucketAdvisor().isPrimaryWithWait()) {
        InternalDistributedMember primaryHolder = brq.getBucketAdvisor()
            .basicGetPrimaryMember();
        throw new PrimaryBucketException("Bucket " + brq.getName()
            + " is not primary. Current primary holder is " + primaryHolder);
      }
    }
      
    return hdfsQueue;
  }

  public RegionEntry getEntryFromEvent(Object key, HDFSGatewayEventImpl event, boolean forceOnHeap, boolean forUpdate) {
    Object val;
    if (event.getOperation().isDestroy()) {
      val = Token.TOMBSTONE;
    } else if (event.getOperation().isInvalidate()) {
      val = Token.INVALID;
    } else {
      val = event.getValue();
    }
    RegionEntry re = null;
    final TXStateInterface tx = owner.getTXState();
    if (tx == null) {
      re = createRegionEntry(key, val, event.getVersionTag(), forceOnHeap);
      return re;
    }
    else
    if (val != null) {
      if (((re = this.backingRM.getEntryInVM(key)) == null)
          || (re.isRemoved() && !re.isTombstone())) {
        boolean shouldCreateOnHeapEntry = !(owner.getOffHeap() && forUpdate); 
        re = createRegionEntry(key, val, event.getVersionTag(), shouldCreateOnHeapEntry);
        if (forUpdate) {
          if (re != null && tx != null) {
            // put the region entry in backing CHM of AbstractRegionMap so that
            // it can be locked in basicPut/destroy
            RegionEntry oldRe = backingRM.putEntryIfAbsent(key, re);
            if (oldRe != null) {
              if (re instanceof OffHeapRegionEntry && !oldRe.equals(re)) {
                ((OffHeapRegionEntry)re).release();
              }
              return oldRe;
            }
            re.setMarkedForEviction();
            owner.updateSizeOnCreate(key,
                owner.calculateRegionEntryValueSize(re));
            ((AbstractRegionMap)backingRM).incEntryCount(1);
            ((AbstractRegionMap)backingRM).lruEntryCreate(re);
          }
        }
      }
    }
    return re;
  }

  public RegionEntry getEntryFromEvent(Object key, SortedHoplogPersistedEvent event, boolean forceOnHeap, boolean forUpdate) {
    Object val = getValueFromEvent(event);
    RegionEntry re = null;
    final TXStateInterface tx = owner.getTXState();
    if (tx == null) {
      re = createRegionEntry(key, val, event.getVersionTag(), forceOnHeap);
      return re;
    }
    else // FOR TX case, we need to create off heap entry if required
    if (val != null) {
      if (((re = this.backingRM.getEntryInVM(key)) == null)
          || (re.isRemoved() && !re.isTombstone())) {
        boolean shouldCreateOnHeapEntry = !(owner.getOffHeap() && forUpdate); 
        re = createRegionEntry(key, val, event.getVersionTag(), shouldCreateOnHeapEntry);
        if(forUpdate) {
          if (re != null && tx != null) {
            // put the region entry in backing CHM of AbstractRegionMap so that
            // it can be locked in basicPut/destroy
            RegionEntry oldRe = backingRM.putEntryIfAbsent(key, re);
            if (oldRe != null) {
              if (re instanceof OffHeapRegionEntry && !oldRe.equals(re)) {
                ((OffHeapRegionEntry)re).release();
              }
              return oldRe;
            }
            re.setMarkedForEviction();
            owner.updateSizeOnCreate(key,
                owner.calculateRegionEntryValueSize(re));
            ((AbstractRegionMap)backingRM).incEntryCount(1);
            ((AbstractRegionMap)backingRM).lruEntryCreate(re);
          }
        }
      }
    }
    return re;
  }

  private RegionEntry createRegionEntry(Object key, Object value, VersionTag tag, boolean forceOnHeap) {
    RegionEntryFactory ref = backingRM.getEntryFactory();
    if (forceOnHeap) {
      ref = ref.makeOnHeap();
    }
    value = getValueDuringGII(key, value);
    RegionEntry re = ref.createEntry(this.owner, key, value);
    setVersionTag(re, tag);
    if (re instanceof LRUEntry) {
      assert backingRM instanceof AbstractLRURegionMap;
      EnableLRU ccHelper = ((AbstractLRURegionMap)backingRM)._getCCHelper();
      ((LRUEntry)re).updateEntrySize(ccHelper);
    }
    return re;
  }

  private Object getValueDuringGII(Object key, Object value) {
    if (owner.getIndexUpdater() != null && !owner.isInitialized()) {
      return AbstractRegionMap.listOfDeltasCreator.newValue(key, owner, value,
          null);
    }
    return value;
  }

  private Set createEntriesSet(IteratorType type)
      throws ForceReattemptException {
    ConcurrentParallelGatewaySenderQueue q = getHDFSQueue();
    if (q == null) return Collections.emptySet();
    HDFSBucketRegionQueue brq = q.getBucketRegionQueue(this.owner.getPartitionedRegion(), owner.getId());
    return new HDFSEntriesSet(owner, brq, owner.getHoplogOrganizer(), type, refs);
  }

  private void closeDeadIterators() {
    Reference<? extends HDFSIterator> weak;
    while ((weak = refs.poll()) != null) {
      if (logger.isTraceEnabled() || DEBUG) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Closing weak ref for iterator "
            + weak.get()));
      }
      weak.get().close();
    }
  }

  /**
   * gets the value from event, deserializing if necessary.
   */
  private Object getValueFromEvent(PersistedEventImpl ev) {
    if (ev.getOperation().isDestroy()) {
      return Token.TOMBSTONE;
    } else if (ev.getOperation().isInvalidate()) {
      return Token.INVALID;
    }
    return ev.getValue();
  }
}
