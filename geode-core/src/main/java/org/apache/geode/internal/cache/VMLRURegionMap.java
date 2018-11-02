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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.eviction.AbstractEvictionController;
import org.apache.geode.internal.cache.eviction.CachedDeserializableValueWrapper;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.eviction.EvictionListBuilder;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.StoredObject;

/**
 * Internal implementation of {@link RegionMap} for regions stored in normal VM memory that maintain
 * an LRU.
 *
 * @since GemFire 3.5.1
 */
// TODO: change back from public to package-private
public class VMLRURegionMap extends AbstractRegionMap {
  private static final Logger logger = LogService.getLogger();

  public VMLRURegionMap(EvictableRegion owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    this(owner, attr, internalRegionArgs, createEvictionController(owner, internalRegionArgs));
  }

  public VMLRURegionMap(EvictableRegion owner, Attributes attr,
      InternalRegionArguments internalRegionArgs, EvictionController evictionController) {
    super(internalRegionArgs);
    initialize(owner, attr, internalRegionArgs);
    this.evictionController = evictionController;
    getEvictionController().setPerEntryOverhead(getEntryOverhead());
    this.lruList = new EvictionListBuilder(getEvictionController()).create();
  }

  private final EvictionController evictionController;

  /** The list of nodes in LRU order */
  private final EvictionList lruList;

  public EvictionList getEvictionList() {
    return this.lruList;
  }

  @Override
  public EvictionController getEvictionController() {
    return evictionController;
  }

  protected void initialize(EvictableRegion evictableRegion, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    if (evictableRegion instanceof InternalRegion) {
      initialize((InternalRegion) evictableRegion, attr, internalRegionArgs, true);
    } else {
      initialize((PlaceHolderDiskRegion) evictableRegion, attr, internalRegionArgs, true);
    }
  }

  @Override
  public void setEntryFactory(RegionEntryFactory f) {
    super.setEntryFactory(f);
    if (getEvictionController() != null) {
      // any time the entry factory changes we need to recalculate the entry overhead
      getEvictionController().setPerEntryOverhead(getEntryOverhead());
    }
  }


  private static EvictionController createEvictionController(EvictableRegion owner,
      InternalRegionArguments internalRegionArgs) {
    EvictionController controller = owner.getExistingController(internalRegionArgs);
    if (controller == null) {
      controller = AbstractEvictionController.create(owner.getEvictionAttributes(),
          owner.getOffHeap(), owner.getStatisticsFactory(), owner.getNameForStats());
    }
    return controller;
  }

  /**
   * The delta produced during a put for activating LRU cannot be used while some outside party
   * (LocalRegion) has a segment locked... so we'll keep it in a thread local for a callback after
   * the segment is released.
   */
  private final ThreadLocal lruDelta = new ThreadLocal();
  private final ThreadLocal mustRemove = new ThreadLocal();
  private final ThreadLocal callbackDisabled = new ThreadLocal();

  private int getDelta() {
    Object d = lruDelta.get();
    lruDelta.set(null); // We only want the delta consumed once
    if (d == null)
      return 0;
    return ((Integer) d).intValue();
  }

  private void setDelta(int delta) {
    if (!getCallbackDisabled()) {
      if (getMustRemove()) {
        // after implementation of network-partition-detection we may
        // run into situations where a cache listener performs a cache
        // operation that needs to update LRU stats. In order to do this
        // we first have to execute the previous LRU actions.
        lruUpdateCallback();
      }
      setMustRemove(true);
    }
    Integer delt = (Integer) lruDelta.get();
    if (delt != null) {
      delta += delt.intValue();
    }
    lruDelta.set(Integer.valueOf(delta));
  }

  /**
   * Used when a CachedDeserializable's value changes form. PRECONDITION: caller has le synced
   *
   * @param le the entry whose CachedDeserializable's value changed.
   * @param cd the CachedDeserializable whose form has changed
   * @param v the new form of the CachedDeserializable's value.
   * @return true if finishChangeValueForm needs to be called
   * @since GemFire 6.1.2.9
   */
  @Override
  public boolean beginChangeValueForm(EvictableEntry le, CachedDeserializable cd, Object v) {
    // make sure this cached deserializable is still in the entry
    {
      if (getEvictionController().getEvictionAlgorithm().isLRUEntry()) {
        // no need to worry about the value changing form with entry LRU.
        return false;
      }
      Object curVal = le.getValue(); // OFFHEAP: _getValue ok
      if (curVal != cd) {
        if (cd instanceof StoredObject) {
          if (!cd.equals(curVal)) {
            return false;
          }
        } else {
          return false;
        }
      }
    }
    boolean result = false;
    int delta =
        le.updateEntrySize(getEvictionController(), new CachedDeserializableValueWrapper(v));
    if (delta != 0) {
      result = true;
      boolean disabledLURCallbacks = disableLruUpdateCallback();
      // by making sure that callbacks are disabled when we call
      // setDelta; it ensures that the setDelta will just inc the delta
      // value and not call lruUpdateCallback which we call in
      // finishChangeValueForm
      setDelta(delta);
      if (disabledLURCallbacks) {
        enableLruUpdateCallback();
      }
    }
    // fix for bug 42090
    if (getEvictionController().getEvictionAlgorithm().isLRUHeap() && _isOwnerALocalRegion()
        && _getOwner() instanceof BucketRegion
        && HeapEvictor.EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST) {
      result = false;
    }
    return result;
  }

  /**
   * @since GemFire 6.1.2.9
   */
  @Override
  public void finishChangeValueForm() {
    lruUpdateCallback();
  }

  private boolean getMustRemove() {
    Object d = mustRemove.get();
    if (d == null)
      return false;
    return ((Boolean) d).booleanValue();
  }

  private void setMustRemove(boolean b) {
    mustRemove.set(b ? Boolean.TRUE : null);
  }

  private boolean getCallbackDisabled() {
    Object d = callbackDisabled.get();
    if (d == null)
      return false;
    return ((Boolean) d).booleanValue();
  }

  private void setCallbackDisabled(boolean b) {
    callbackDisabled.set(b ? Boolean.TRUE : Boolean.FALSE);
  }

  /**
   * Evicts the given entry from the cache. Returns the total number of bytes evicted. 1. For action
   * local destroy, returns size(key + value) 2. For action evict to disk, returns size(value)
   *
   * @return number of bytes evicted, zero if no eviction took place
   */
  protected int evictEntry(EvictableEntry entry, EvictionCounters stats)
      throws RegionClearedException {
    EvictionAction action = getEvictionController().getEvictionAction();
    LocalRegion region = _getOwner();
    if (action.isLocalDestroy()) {
      int size = entry.getEntrySize();
      if (region.evictDestroy(entry)) {
        stats.incDestroys();
        return size;
      } else {
        return 0;
      }
    } else if (action.isOverflowToDisk()) {
      Assert.assertTrue(entry instanceof DiskEntry);
      int change = 0;
      synchronized (entry) {
        if (entry.isInUseByTransaction()) {
          entry.unsetEvicted();
          if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
            logger.trace(LogMarker.LRU_VERBOSE, "No eviction of transactional entry for key={}",
                entry.getKey());
          }
          return 0;
        }

        // Do the following check while synchronized to fix bug 31761
        Token entryVal = entry.getValueAsToken();
        if (entryVal == null) {
          if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
            logger.trace(LogMarker.LRU_VERBOSE, "no need to evict already evicted key={}",
                entry.getKey());
          }
          return 0;
        }
        if (Token.isInvalidOrRemoved(entryVal)) {
          // no need to evict these; it will not save any space
          // and the destroyed token needs to stay in memory
          if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
            logger.trace(LogMarker.LRU_VERBOSE, "no need to evict {} token for key={}", entryVal,
                entry.getKey());
          }
          return 0;
        }
        entry.setEvicted();
        change =
            DiskEntry.Helper.overflowToDisk((DiskEntry) entry, region, getEvictionController());
      }
      boolean result = change < 0;
      if (result) {

        if (_getOwner() instanceof BucketRegion) {
          BucketRegion bucketRegion = (BucketRegion) _getOwner();
          bucketRegion.updateCounter(change);
          stats.updateCounter(change);
        } else {
          stats.updateCounter(change);
        }

      } else {
        if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
          logger.trace(LogMarker.LRU_VERBOSE,
              "no need to evict token for key={} because moving its value to disk resulted in a net change of {} bytes.",
              entry.getKey(), change);
        }
      }
      return change * -1;

    } else {
      throw new InternalGemFireException(
          String.format("Unknown eviction action: %s", action));
    }
  }

  /**
   * update the running counter of all the entries
   *
   * @param delta Description of the Parameter
   */
  protected void changeTotalEntrySize(int delta) {
    if (_isOwnerALocalRegion()) {
      if (_getOwner() instanceof BucketRegion) {
        BucketRegion bucketRegion = (BucketRegion) _getOwner();
        bucketRegion.updateCounter(delta);
      }
    }
    getEvictionList().getStatistics().updateCounter(delta);

    if (delta > 0) {
      if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
        logger.trace(LogMarker.LRU_VERBOSE, "total lru size is now: {}", getTotalEntrySize());
      }
    }
  }

  @Override
  public void evictValue(Object key) {
    throw new IllegalStateException(
        "The evictValue is not supported on regions with eviction attributes.");
  }

  /**
   * Gets the total entry size limit for the map from the capacity controller helper.
   *
   * @return The total allowable size of this maps entries.
   */
  protected long getLimit() {
    if (_getOwner() instanceof BucketRegion) {
      BucketRegion bucketRegion = (BucketRegion) _getOwner();
      return bucketRegion.getLimit();
    }
    return getEvictionController().getCounters().getLimit();
  }

  public EvictionCounters getLRUStatistics() {
    return getEvictionController().getCounters();
  }


  /**
   * return the current size of all the entries.
   *
   * @return The current size of all the entries.
   */
  protected long getTotalEntrySize() {
    if (_getOwnerObject() instanceof BucketRegion) {
      BucketRegion bucketRegion = (BucketRegion) _getOwner();
      return bucketRegion.getCounter();
    }
    return getEvictionController().getCounters().getCounter();
  }

  @Override
  public void lruUpdateCallback() {
    final boolean isDebugEnabled_LRU = logger.isTraceEnabled(LogMarker.LRU_VERBOSE);

    if (getCallbackDisabled()) {
      return;
    }
    final int delta = getDelta();
    int bytesToEvict = delta;
    resetThreadLocals();
    if (isDebugEnabled_LRU && _isOwnerALocalRegion()) {
      logger.trace(LogMarker.LRU_VERBOSE,
          "lruUpdateCallback; list size is: {}; actual size is: {}; map size is: {}; delta is: {}; limit is: {}; tombstone count={}",
          getTotalEntrySize(), this.getEvictionList().size(), size(), delta, getLimit(),
          _getOwner().getTombstoneCount());
    }
    EvictionCounters stats = getEvictionList().getStatistics();
    if (!_isOwnerALocalRegion()) {
      changeTotalEntrySize(delta);
      // instead of evicting we just quit faulting values in
    } else if (getEvictionController().getEvictionAlgorithm().isLRUHeap()) {
      changeTotalEntrySize(delta);
      try {
        while (bytesToEvict > 0
            && getEvictionController().mustEvict(stats, _getOwner(), bytesToEvict)) {
          boolean evictFromThisRegion = true;
          if (HeapEvictor.EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST
              && _getOwner() instanceof BucketRegion) {
            long bytesEvicted = 0;
            long totalBytesEvicted = 0;
            List<BucketRegion> regions =
                ((BucketRegion) _getOwner()).getPartitionedRegion().getSortedBuckets();
            Iterator<BucketRegion> iter = regions.iterator();
            while (iter.hasNext()) {
              BucketRegion region = iter.next();
              // only primaries can trigger inline eviction fix for 41814
              if (!region.getBucketAdvisor().isPrimary()) {
                try {
                  bytesEvicted = region.getRegionMap().centralizedLruUpdateCallback();
                  if (bytesEvicted == 0) {
                    iter.remove();
                  } else {
                    evictFromThisRegion = false;
                  }
                  totalBytesEvicted += bytesEvicted;
                  bytesToEvict -= bytesEvicted;
                  if (bytesEvicted > bytesToEvict) {
                    bytesToEvict = 0;
                    break;
                  }
                  if (totalBytesEvicted > bytesToEvict) {
                    break;
                  }
                } catch (RegionDestroyedException rd) {
                  region.cache.getCancelCriterion().checkCancelInProgress(rd);
                } catch (Exception e) {
                  region.cache.getCancelCriterion().checkCancelInProgress(e);
                  logger.warn(String.format("Exception: %s occurred during eviction ",
                      new Object[] {e.getMessage()}),
                      e);
                }
              }
            }
          }
          if (evictFromThisRegion) {
            EvictableEntry removalEntry = getEvictionList().getEvictableEntry();
            if (removalEntry != null) {
              int sizeOfValue = evictEntry(removalEntry, stats);
              if (sizeOfValue != 0) {
                bytesToEvict -= sizeOfValue;
                if (isDebugEnabled_LRU) {
                  logger.trace(LogMarker.LRU_VERBOSE,
                      "evicted entry key={} total entry size is now: {} bytesToEvict :{}",
                      removalEntry.getKey(), getTotalEntrySize(), bytesToEvict);
                }
                stats.incEvictions();
                if (_isOwnerALocalRegion()) {
                  _getOwner().incBucketEvictions();
                }
                if (isDebugEnabled_LRU) {
                  logger.trace(LogMarker.LRU_VERBOSE, "evictions={}", stats.getEvictions());
                }
              }

            } else {
              if (getTotalEntrySize() != 0) {
                if (isDebugEnabled_LRU) {
                  logger.trace(LogMarker.LRU_VERBOSE, "leaving evict loop early");
                }
              }
              break;
            }
          }
        }
      } catch (RegionClearedException e) {
        if (isDebugEnabled_LRU) {
          logger.trace(LogMarker.LRU_VERBOSE, "exception ={}", e.getCause().getMessage(),
              e.getCause());
        }
      }
    } else {
      try {
        // to fix bug 48285 do no evict if bytesToEvict <= 0.
        while (bytesToEvict > 0
            && getEvictionController().mustEvict(stats, _getOwner(), bytesToEvict)) {
          EvictableEntry removalEntry = (EvictableEntry) getEvictionList().getEvictableEntry();
          if (removalEntry != null) {
            if (evictEntry(removalEntry, stats) != 0) {
              if (isDebugEnabled_LRU) {
                logger.trace(LogMarker.LRU_VERBOSE,
                    "evicted entry key(2)={} total entry size is now: {} bytesToEvict: {}",
                    removalEntry.getKey(), getTotalEntrySize(), bytesToEvict);
              }
              stats.incEvictions();
              if (_isOwnerALocalRegion()) {
                _getOwner().incBucketEvictions();
              }
              if (isDebugEnabled_LRU) {
                logger.trace(LogMarker.LRU_VERBOSE, "evictions={}", stats.getEvictions());
              }

            }

          } else {
            if (getTotalEntrySize() != 0) {
              if (isDebugEnabled_LRU) {
                logger.trace(LogMarker.LRU_VERBOSE, "leaving evict loop early");
              }
            }
            break;
          }
        }
        changeTotalEntrySize(delta);
      } catch (RegionClearedException e) {
        if (isDebugEnabled_LRU) {
          logger.debug("exception ={}", e.getCause().getMessage(), e.getCause());
        }
      }
    }
    if (isDebugEnabled_LRU) {
      logger.trace(LogMarker.LRU_VERBOSE, "callback complete.  LRU size is now {}",
          getEvictionController().getCounters().getCounter());
    }
    // If in transaction context (either local or message)
    // reset the tx thread local
  }

  private boolean mustEvict() {
    LocalRegion owner = _getOwner();
    InternalResourceManager resourceManager = owner.getCache().getInternalResourceManager();
    boolean offheap = owner.getAttributes().getOffHeap();
    return resourceManager.getMemoryMonitor(offheap).getState().isEviction() && this.sizeInVM() > 0;
  }

  @Override
  public int centralizedLruUpdateCallback() {
    final boolean isDebugEnabled_LRU = logger.isTraceEnabled(LogMarker.LRU_VERBOSE);

    int evictedBytes = 0;
    if (getCallbackDisabled()) {
      return evictedBytes;
    }
    getDelta();
    resetThreadLocals();
    if (isDebugEnabled_LRU) {
      logger.trace(LogMarker.LRU_VERBOSE,
          "centralLruUpdateCallback: lru size is now {}, limit is: {}", getTotalEntrySize(),
          getLimit());
    }
    EvictionCounters stats = getEvictionList().getStatistics();
    try {
      while (mustEvict() && evictedBytes == 0) {
        EvictableEntry removalEntry = getEvictionList().getEvictableEntry();
        if (removalEntry != null) {
          evictedBytes = evictEntry(removalEntry, stats);
          if (evictedBytes != 0) {
            _getOwner().incBucketEvictions();
            stats.incEvictions();
            if (isDebugEnabled_LRU) {
              logger.debug("evictions={}", stats.getEvictions());
            }
          }
        } else {
          if (getTotalEntrySize() != 0) {
            if (isDebugEnabled_LRU) {
              logger.trace(LogMarker.LRU_VERBOSE, "leaving evict loop early");
            }
          }
          break;
        }
      }
    } catch (RegionClearedException e) {
      // Ignore
      if (isDebugEnabled_LRU) {
        logger.trace(LogMarker.LRU_VERBOSE, "exception ={}", e.getCause().getMessage(),
            e.getCause());
      }
    }
    if (isDebugEnabled_LRU) {
      logger.trace(LogMarker.LRU_VERBOSE, "callback complete");
    }
    // If in transaction context (either local or message)
    // reset the tx thread local
    return evictedBytes;
  }



  /**
   * Update counter related to limit in list
   *
   * @since GemFire 5.7
   */
  @Override
  public void updateEvictionCounter() {
    final int delta = getDelta();
    resetThreadLocals();
    if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
      logger.trace(LogMarker.LRU_VERBOSE, "updateStats - delta is: {} total is: {} limit is: {}",
          delta, getTotalEntrySize(), getLimit());
    }

    if (delta != 0) {
      changeTotalEntrySize(delta);
    }
  }

  @Override
  public boolean disableLruUpdateCallback() {
    if (getCallbackDisabled()) {
      return false;
    } else {
      setCallbackDisabled(true);
      return true;
    }
  }

  @Override
  public void enableLruUpdateCallback() {
    setCallbackDisabled(false);
  }

  @Override
  public void resetThreadLocals() {
    mustRemove.set(null);
    lruDelta.set(null);
    callbackDisabled.set(null);
  }

  @Override
  public Set<VersionSource> clear(RegionVersionVector rvv, BucketRegion bucketRegion) {
    getEvictionList().clear(rvv, bucketRegion);
    return super.clear(rvv, bucketRegion);
  }

  /*
   * Asif : Motivation: An entry which is no longer existing in the system due to clear operation,
   * should not be present the LRUList being used by the region.
   *
   * Case1 : An entry has been written to disk & on its return code path, it invokes lruCreate or
   * lruUpdate. Before starting the operation of writing to disk, the HTree reference is set in the
   * threadlocal. A clear operation changes the Htree reference in a write lock. Thus if the htree
   * reference has not changed till this point, it would mean either the entry is still valid or a
   * clear operation is in progress but has not changed the Htree Reference . Since we store the
   * LRUList in a local variable, it implies that if clear occurs , it will go in the stale list &
   * if not it goes in the right list. Both ways we are safe.
   *
   * Case 2: The Htree reference has changed ( implying a clear conflic with put) but the entry is
   * valid. This is possible as we first set the Htree Ref in thread local. Now before the update
   * operation has acquired the entry , clear happens. As a result the update operation has become
   * create. Since the clear changes the Htree Ref & the LRUList in a write lock & hence by the time
   * the original update operation acquires the read lock, the LRUList has already been changed by
   * clear. Now in the update operation's return path the List which it stores in local variable is
   * bound to be the new List. Since our code checks if the entry reference exists in the region in
   * case of conflict & if yes, we append the entry to the List. It is guaranteed to be added to the
   * new List.
   *
   * Also it is necessary that when we clear the region, first the concurrent map of the region
   * containing entries needs to be cleared. The Htree Reference should be reset after that. And
   * then we should be resetting the LRUList. Previously the Htree reference was being set before
   * clearing the Map. This caused Bug 37606. If the order of clear operation on disk region is (
   * incorrect ) 1) map.clear 2) Resetting the LRUList 3) Changing the Htree ref Then following bug
   * can occur., During entry operation on its return path, invokes lruUpdate/lruCreate. By that
   * time the clear proceeds & it has reset the LRUList & cleared the entries. But as the Htree ref
   * has not changed, we would take the locally available LRUList ( which may be the new List) &
   * append the entry to the List.
   *
   *
   *
   */
  @Override
  public void lruEntryCreate(RegionEntry re) {
    EvictableEntry e = (EvictableEntry) re;
    if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
      logger.trace(LogMarker.LRU_VERBOSE,
          "lruEntryCreate for key={}; list size is: {}; actual size is: {}; map size is: {}; entry size: {}; in lru clock: {}",
          re.getKey(), getTotalEntrySize(), this.getEvictionList().size(), size(), e.getEntrySize(),
          !e.isEvicted());
    }
    e.unsetEvicted();
    EvictionList lruList = getEvictionList();
    DiskRegion disk = _getOwner().getDiskRegion();
    boolean possibleClear = disk != null && disk.didClearCountChange();
    if (!possibleClear || this._getOwner().basicGetEntry(re.getKey()) == re) {
      lruList.appendEntry(e);
      lruEntryUpdate(e);
    }
  }

  @Override
  public void lruEntryUpdate(RegionEntry re) {
    final EvictableEntry e = (EvictableEntry) re;
    setDelta(e.updateEntrySize(getEvictionController()));
    if (logger.isDebugEnabled()) {
      logger.debug("lruEntryUpdate for key={} size={}", re.getKey(), e.getEntrySize());
    }
    EvictionList lruList = getEvictionList();
    if (_isOwnerALocalRegion()) {
      DiskRegion disk = _getOwner().getDiskRegion();
      boolean possibleClear = disk != null && disk.didClearCountChange();
      if (!possibleClear || this._getOwner().basicGetEntry(re.getKey()) == re) {
        if (e instanceof DiskEntry) {
          if (!e.isEvicted()) {
            lruList.appendEntry(e);
          }
        }
        // Why reset the refcount? All the txs that currently reference
        // this region entry still do but they will now fail with conflicts.
        // But they also have logic in them to dec the refcount.
        // I think we did the resetRefCount thinking it was safe
        // to drop it back to zero since the RE was modified and give
        // us a chance to evict it. But if the txs that are going to fail
        // with a conflict still do refCountDecs then after the reset any
        // new txs that inc the refcount may have their count decd by one of
        // the old txs allowing the entry to be evicted and causing a tx conflict.
        // TODO: this should now be safe but why the odd condition for this block
        // and why call lruList.appendEntry twice (once above and once in resetRefCount.
        // Also lruEntryUpdate only happens on an lru. Do we need to call reset for the non-lru
        // (expiration) case?
        e.resetRefCount(lruList);
      }
    } else {
      // We are recovering the region so it is a DiskEntry.
      // Also clear is not yet possible and this entry will be in the region.
      // No need to call resetRefCount since tx are not yet possible.
      if (!e.isEvicted()) {
        lruList.appendEntry(e);
      }
    }
  }

  @Override
  public void lruEntryDestroy(RegionEntry regionEntry) {
    final EvictableEntry e = (EvictableEntry) regionEntry;
    if (logger.isTraceEnabled(LogMarker.LRU_VERBOSE)) {
      logger.trace(LogMarker.LRU_VERBOSE,
          "lruEntryDestroy for key={}; list size is: {}; actual size is: {}; map size is: {}; entry size: {}; in lru clock: {}",
          regionEntry.getKey(), getTotalEntrySize(), this.getEvictionList().size(), size(),
          e.getEntrySize(), !e.isEvicted());
    }

    getEvictionList().destroyEntry(e);
    changeTotalEntrySize(-1 * e.getEntrySize());// subtract the size.
    Token vTok = regionEntry.getValueAsToken();
    if (vTok == Token.DESTROYED || vTok == Token.TOMBSTONE) {
      // OFFHEAP noop TODO: use re.isDestroyedOrTombstone
      // if in token mode we need to recalculate the size of the entry since it's
      // staying in the map and may be resurrected
      e.updateEntrySize(getEvictionController());
    }
  }

  /**
   * Called by DiskEntry.Helper.faultInValue
   */
  @Override
  public void lruEntryFaultIn(EvictableEntry e) {
    if (logger.isDebugEnabled()) {
      logger.debug("lruEntryFaultIn for key={} size={}", e.getKey(), e.getEntrySize());
    }
    EvictionList lruList = getEvictionList();
    if (_isOwnerALocalRegion()) {
      DiskRegion disk = _getOwner().getDiskRegion();
      boolean possibleClear = disk != null && disk.didClearCountChange();
      if (!possibleClear || this._getOwner().basicGetEntry(e.getKey()) == e) {
        lruEntryUpdate(e);
        e.unsetEvicted();
        lruList.appendEntry(e);
      }
    } else {
      lruEntryUpdate(e);
      lruList.appendEntry(e);
    }
  }

  @Override
  public void decTxRefCount(RegionEntry re) {
    LocalRegion lr = null;
    if (_isOwnerALocalRegion()) {
      lr = _getOwner();
    }
    re.decRefCount(getEvictionList(), lr);
  }

  @Override
  public boolean lruLimitExceeded(DiskRegionView diskRegionView) {
    return getEvictionController().lruLimitExceeded(getEvictionController().getCounters(),
        diskRegionView);
  }

  @Override
  public void lruCloseStats() {
    getEvictionList().closeStats();
  }

  @Override
  public boolean confirmEvictionDestroy(RegionEntry regionEntry) {
    // We assume here that a LRURegionMap contains LRUEntries
    EvictableEntry lruRe = (EvictableEntry) regionEntry;
    if (lruRe.isInUseByTransaction() || lruRe.isDestroyed()) {
      lruRe.unsetEvicted();
      return false;
    } else {
      return true;
    }
  }

  @Override
  public long getEvictions() {
    return this.getEvictionController().getCounters().getEvictions();
  }

  @Override
  public void incRecentlyUsed() {
    getEvictionList().incrementRecentlyUsed();
  }
}
