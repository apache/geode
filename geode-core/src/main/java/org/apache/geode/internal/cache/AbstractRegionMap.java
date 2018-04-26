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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.entries.AbstractOplogDiskRegionEntry;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.map.CacheModificationLock;
import org.apache.geode.internal.cache.map.FocusedRegionMap;
import org.apache.geode.internal.cache.map.RegionMapDestroy;
import org.apache.geode.internal.cache.map.RegionMapPut;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.region.entry.RegionEntryFactoryBuilder;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.internal.size.ReflectionSingleObjectSizer;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

/**
 * Abstract implementation of {@link RegionMap}that has all the common behavior.
 *
 * @since GemFire 3.5.1
 */
public abstract class AbstractRegionMap
    implements RegionMap, FocusedRegionMap, CacheModificationLock {
  private static final Logger logger = LogService.getLogger();

  /** The underlying map for this region. */
  protected ConcurrentMapWithReusableEntries<Object, Object> map;

  /**
   * This test hook is used to force the conditions during entry destroy. This hook is used by
   * DestroyEntryWithConcurrentOperationJUnitTest.
   */
  static Runnable testHookRunnableForConcurrentOperation = null;

  private RegionEntryFactory entryFactory;

  private Attributes attr;

  // the region that owns this map
  private RegionMapOwner owner;

  private final EntryEventSerialization entryEventSerialization = new EntryEventSerialization();

  protected AbstractRegionMap(InternalRegionArguments internalRegionArgs) {
    // do nothing
  }

  protected void initialize(RegionMapOwner owner, Attributes attr,
      InternalRegionArguments internalRegionArgs, boolean isLRU) {
    _setAttributes(attr);
    setOwner(owner);
    setEntryMap(createConcurrentMapWithReusableEntries(attr.initialCapacity, attr.loadFactor,
        attr.concurrencyLevel, false, new AbstractRegionEntry.HashRegionEntryCreator()));

    boolean isDisk;
    boolean withVersioning;
    boolean offHeap;
    if (owner instanceof InternalRegion) {
      InternalRegion region = (InternalRegion) owner;
      isDisk = region.getDiskRegion() != null;
      withVersioning = region.getConcurrencyChecksEnabled();
      offHeap = region.getOffHeap();
    } else if (owner instanceof PlaceHolderDiskRegion) {
      offHeap = ((RegionEntryContext) owner).getOffHeap();
      isDisk = true;
      withVersioning =
          ((DiskRegionView) owner).getFlags().contains(DiskRegionFlag.IS_WITH_VERSIONING);
    } else {
      throw new IllegalStateException("expected LocalRegion or PlaceHolderDiskRegion");
    }

    setEntryFactory(new RegionEntryFactoryBuilder().create(attr.statisticsEnabled, isLRU, isDisk,
        withVersioning, offHeap));
  }

  private ConcurrentMapWithReusableEntries<Object, Object> createConcurrentMapWithReusableEntries(
      int initialCapacity, float loadFactor, int concurrencyLevel, boolean isIdentityMap,
      CustomEntryConcurrentHashMap.HashEntryCreator<Object, Object> entryCreator) {
    if (entryCreator != null) {
      return new CustomEntryConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel,
          isIdentityMap, entryCreator);
    } else {
      return new CustomEntryConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel,
          isIdentityMap);
    }
  }

  @Override
  public void changeOwner(LocalRegion r) {
    if (r == _getOwnerObject()) {
      return;
    }
    setOwner(r);
  }

  @Override
  public void setEntryFactory(RegionEntryFactory f) {
    this.entryFactory = f;
  }

  @Override
  public RegionEntryFactory getEntryFactory() {
    return this.entryFactory;
  }

  private void _setAttributes(Attributes a) {
    this.attr = a;
  }

  @Override
  public Attributes getAttributes() {
    return this.attr;
  }

  public LocalRegion _getOwner() {
    return (LocalRegion) this.owner;
  }

  boolean _isOwnerALocalRegion() {
    return this.owner instanceof LocalRegion;
  }

  Object _getOwnerObject() {
    return this.owner;
  }

  /**
   * Tells this map what region owns it.
   */
  private void setOwner(RegionMapOwner owner) {
    this.owner = owner;
  }

  @Override
  public ConcurrentMapWithReusableEntries<Object, Object> getCustomEntryConcurrentHashMap() {
    return map;
  }

  @Override
  public Map<Object, Object> getEntryMap() {
    return map;
  }

  @Override
  public void setEntryMap(ConcurrentMapWithReusableEntries<Object, Object> map) {
    this.map = map;
  }

  @Override
  public int size() {
    return getEntryMap().size();
  }

  // this is currently used by stats and eviction
  @Override
  public int sizeInVM() {
    return getEntryMap().size();
  }

  @Override
  public boolean isEmpty() {
    return getEntryMap().isEmpty();
  }

  @Override
  public Set keySet() {
    return getEntryMap().keySet();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Collection<RegionEntry> regionEntries() {
    return (Collection) getEntryMap().values();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Collection<RegionEntry> regionEntriesInVM() {
    return (Collection) getEntryMap().values();
  }

  @Override
  public boolean containsKey(Object key) {
    RegionEntry re = getEntry(key);
    if (re == null) {
      return false;
    }
    if (re.isRemoved()) {
      return false;
    }
    return true;
  }

  @Override
  public RegionEntry getEntry(Object key) {
    RegionEntry re = (RegionEntry) getEntryMap().get(key);
    return re;
  }

  @Override
  public RegionEntry getEntry(EntryEventImpl event) {
    return getEntry(event.getKey());
  }


  @Override
  public RegionEntry getEntryInVM(Object key) {
    return (RegionEntry) getEntryMap().get(key);
  }


  @Override
  public RegionEntry putEntryIfAbsent(Object key, RegionEntry regionEntry) {
    RegionEntry oldRe = (RegionEntry) getEntryMap().putIfAbsent(key, regionEntry);
    if (oldRe == null && (regionEntry instanceof OffHeapRegionEntry) && _isOwnerALocalRegion()
        && _getOwner().isThisRegionBeingClosedOrDestroyed()) {
      // prevent orphan during concurrent destroy (#48068)
      Object v = regionEntry.getValue();
      if (v != Token.REMOVED_PHASE1 && v != Token.REMOVED_PHASE2 && v instanceof StoredObject
          && ((StoredObject) v).hasRefCount()) {
        if (getEntryMap().remove(key, regionEntry)) {
          ((OffHeapRegionEntry) regionEntry).release();
        }
      }
    }
    return oldRe;
  }

  @Override
  public RegionEntry getOperationalEntryInVM(Object key) {
    RegionEntry re = (RegionEntry) getEntryMap().get(key);
    return re;
  }


  @Override
  public void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat) {
    if (regionEntry.isTombstone() && getEntryMap().get(key) == regionEntry) {
      logger.fatal(
          LocalizedMessage.create(LocalizedStrings.AbstractRegionMap_ATTEMPT_TO_REMOVE_TOMBSTONE),
          new Exception("stack trace"));
      return; // can't remove tombstones except from the tombstone sweeper
    }
    if (getEntryMap().remove(key, regionEntry)) {
      regionEntry.removePhase2();
      if (updateStat) {
        incEntryCount(-1);
      }
    }
  }

  @Override
  public void removeEntry(Object key, RegionEntry regionEntry, boolean updateStat,
      EntryEventImpl event, final InternalRegion internalRegion) {
    boolean success = false;
    if (regionEntry.isTombstone() && getEntryMap().get(key) == regionEntry) {
      logger.fatal(
          LocalizedMessage.create(LocalizedStrings.AbstractRegionMap_ATTEMPT_TO_REMOVE_TOMBSTONE),
          new Exception("stack trace"));
      return; // can't remove tombstones except from the tombstone sweeper
    }
    if (getEntryMap().remove(key, regionEntry)) {
      regionEntry.removePhase2();
      success = true;
      if (updateStat) {
        incEntryCount(-1);
      }
    }
  }

  protected void incEntryCount(int delta) {
    LocalRegion lr = _getOwner();
    if (lr != null) {
      CachePerfStats stats = lr.getCachePerfStats();
      if (stats != null) {
        stats.incEntryCount(delta);
      }
    }
  }

  void incClearCount(LocalRegion lr) {
    if (lr != null && !(lr instanceof HARegion)) {
      CachePerfStats stats = lr.getCachePerfStats();
      if (stats != null) {
        stats.incClearCount();
      }
    }
  }

  private void _mapClear() {
    Executor executor = null;
    InternalCache cache = this.owner.getCache();
    if (cache != null) {
      DistributionManager manager = cache.getDistributionManager();
      if (manager != null) {
        executor = manager.getWaitingThreadPool();
      }
    }
    getCustomEntryConcurrentHashMap().clearWithExecutor(executor);
  }

  @Override
  public void close(BucketRegion bucketRegion) {
    clear(null, bucketRegion);
  }

  /**
   * Clear the region and, if an RVV is given, return a collection of the version sources in all
   * remaining tags
   */
  @Override
  public Set<VersionSource> clear(RegionVersionVector rvv, BucketRegion bucketRegion) {
    Set<VersionSource> result = new HashSet<VersionSource>();

    if (!_isOwnerALocalRegion()) {
      // Fix for #41333. Just clear the the map
      // if we failed during initialization.
      _mapClear();
      return null;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Clearing entries for {} rvv={}", _getOwner(), " rvv=" + rvv);
    }
    LocalRegion lr = _getOwner();
    RegionVersionVector localRvv = lr.getVersionVector();
    incClearCount(lr);
    // lock for size calcs if the region might have tombstones
    Object lockObj = lr.getConcurrencyChecksEnabled() ? lr.getSizeGuard() : new Object();
    synchronized (lockObj) {
      if (rvv == null) {
        int delta = 0;
        try {
          delta = sizeInVM(); // TODO soplog need to determine if stats should
                              // reflect only size in memory or the complete thing
        } catch (GemFireIOException e) {
          // ignore rather than throwing an exception during cache close
        }
        int tombstones = lr.getTombstoneCount();
        _mapClear();
        _getOwner().updateSizeOnClearRegion(delta - tombstones);
        _getOwner().incTombstoneCount(-tombstones);
        if (delta != 0) {
          incEntryCount(-delta);
        }
      } else {
        int delta = 0;
        int tombstones = 0;
        VersionSource myId = _getOwner().getVersionMember();
        if (localRvv != rvv) {
          localRvv.recordGCVersions(rvv);
        }
        final boolean isTraceEnabled = logger.isTraceEnabled();
        for (RegionEntry re : regionEntries()) {
          synchronized (re) {
            Token value = re.getValueAsToken();
            // if it's already being removed or the entry is being created we leave it alone
            if (value == Token.REMOVED_PHASE1 || value == Token.REMOVED_PHASE2) {
              continue;
            }

            VersionSource id = re.getVersionStamp().getMemberID();
            if (id == null) {
              id = myId;
            }
            if (rvv.contains(id, re.getVersionStamp().getRegionVersion())) {
              if (isTraceEnabled) {
                logger.trace("region clear op is removing {} {}", re.getKey(),
                    re.getVersionStamp());
              }

              boolean tombstone = re.isTombstone();
              // note: it.remove() did not reliably remove the entry so we use remove(K,V) here
              if (getEntryMap().remove(re.getKey(), re)) {
                if (OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap()) {
                  GatewaySenderEventImpl.release(re.getValue()); // OFFHEAP _getValue ok
                }
                // If this is an overflow only region, we need to free the entry on
                // disk at this point.
                try {
                  re.removePhase1(lr, true);
                } catch (RegionClearedException e) {
                  // do nothing, it's already cleared.
                }
                re.removePhase2();
                lruEntryDestroy(re);
                if (tombstone) {
                  _getOwner().incTombstoneCount(-1);
                  tombstones += 1;
                } else {
                  delta += 1;
                }
              }
            } else { // rvv does not contain this entry so it is retained
              result.add(id);
            }
          }
        }
        _getOwner().updateSizeOnClearRegion(delta);
        incEntryCount(-delta);
        incEntryCount(-tombstones);
        if (logger.isDebugEnabled()) {
          logger.debug("Size after clearing = {}", getEntryMap().size());
        }
        if (isTraceEnabled && getEntryMap().size() < 20) {
          _getOwner().dumpBackingMap();
        }
      }
    }
    return result;
  }

  @Override
  public void lruUpdateCallback() {
    // By default do nothing; LRU maps needs to override this method
  }

  public void lruUpdateCallback(boolean b) {
    // By default do nothing; LRU maps needs to override this method
  }

  @Override
  public boolean disableLruUpdateCallback() {
    // By default do nothing; LRU maps needs to override this method
    return false;
  }

  @Override
  public void enableLruUpdateCallback() {
    // By default do nothing; LRU maps needs to override this method
  }

  @Override
  public void resetThreadLocals() {
    // By default do nothing; LRU maps needs to override this method
  }

  /**
   * Tell an LRU that a new entry has been created
   */
  @Override
  public void lruEntryCreate(RegionEntry e) {
    // do nothing by default
  }

  /**
   * Tell an LRU that an existing entry has been destroyed
   */
  @Override
  public void lruEntryDestroy(RegionEntry regionEntry) {
    // do nothing by default
  }

  /**
   * Tell an LRU that an existing entry has been modified
   */
  @Override
  public void lruEntryUpdate(RegionEntry e) {
    // do nothing by default
  }

  @Override
  public void decTxRefCount(RegionEntry e) {
    LocalRegion lr = null;
    if (_isOwnerALocalRegion()) {
      lr = _getOwner();
    }
    e.decRefCount(null, lr);
  }

  @Override
  public boolean lruLimitExceeded(DiskRegionView diskRegionView) {
    return false;
  }

  @Override
  public void lruCloseStats() {
    // do nothing by default
  }

  @Override
  public void lruEntryFaultIn(EvictableEntry entry) {
    // do nothing by default
  }

  /**
   * Process an incoming version tag for concurrent operation detection. This must be done before
   * modifying the region entry.
   *
   * @param regionEntry the entry that is to be modified
   * @param event the modification to the entry
   * @throws InvalidDeltaException if the event contains a delta that cannot be applied
   * @throws ConcurrentCacheModificationException if the event is in conflict with a previously
   *         applied change
   */
  @Override
  public void processVersionTag(RegionEntry regionEntry, EntryEventImpl event) {
    if (regionEntry.getVersionStamp() != null) {
      regionEntry.getVersionStamp().processVersionTag(event);

      // during initialization we record version tag info to detect ops the
      // image provider hasn't seen
      VersionTag<?> tag = event.getVersionTag();
      if (tag != null && !event.getRegion().isInitialized()) {
        ImageState is = event.getRegion().getImageState();
        if (is != null && !event.getRegion().isUsedForPartitionedRegionBucket()) {
          if (logger.isTraceEnabled()) {
            logger.trace("recording version tag in image state: {}", tag);
          }
          is.addVersionTag(event.getKey(), tag);
        }
      }
    }
  }

  private void processVersionTagForGII(RegionEntry re, LocalRegion owner, VersionTag entryVersion,
      boolean isTombstone, InternalDistributedMember sender, boolean checkConflicts) {

    re.getVersionStamp().processVersionTag(_getOwner(), entryVersion, isTombstone, false,
        owner.getMyId(), sender, checkConflicts);
  }

  @Override
  public void copyRecoveredEntries(RegionMap rm) {
    // We need to sort the tombstones before scheduling them,
    // so that they will be in the correct order.
    OrderedTombstoneMap<RegionEntry> tombstones = new OrderedTombstoneMap<RegionEntry>();
    if (rm != null) {
      ConcurrentMapWithReusableEntries<Object, Object> other = rm.getCustomEntryConcurrentHashMap();
      Iterator<Map.Entry<Object, Object>> it = other.entrySetWithReusableEntries().iterator();
      while (it.hasNext()) {
        Map.Entry<Object, Object> me = it.next();
        it.remove(); // This removes the RegionEntry from "rm" but it does not decrement its
                     // refcount to an offheap value.
        RegionEntry oldRe = (RegionEntry) me.getValue();
        Object key = me.getKey();

        @Retained
        @Released
        Object value = oldRe
            .getValueRetain((RegionEntryContext) ((AbstractRegionMap) rm)._getOwnerObject(), true);

        try {
          if (value == Token.NOT_AVAILABLE) {
            // fix for bug 43993
            value = null;
          }
          if (value == Token.TOMBSTONE && !_getOwner().getConcurrencyChecksEnabled()) {
            continue;
          }
          RegionEntry newRe =
              getEntryFactory().createEntry((RegionEntryContext) _getOwnerObject(), key, value);
          // TODO: passing value to createEntry causes a problem with the disk stats.
          // The disk stats have already been set to track oldRe.
          // So when we call createEntry we probably want to give it REMOVED_PHASE1
          // and then set the value in copyRecoveredEntry it a way that does not
          // change the disk stats. This also depends on DiskEntry.Helper.initialize not changing
          // the stats for REMOVED_PHASE1
          copyRecoveredEntry(oldRe, newRe);
          // newRe is now in this.getCustomEntryConcurrentHashMap().
          if (newRe.isTombstone()) {
            VersionTag tag = newRe.getVersionStamp().asVersionTag();
            tombstones.put(tag, newRe);
          } else {
            _getOwner().updateSizeOnCreate(key, _getOwner().calculateRegionEntryValueSize(newRe));
            if (_getOwner() instanceof BucketRegionQueue) {
              BucketRegionQueue brq = (BucketRegionQueue) _getOwner();
              brq.incSecondaryQueueSize(1);
            }
          }
          incEntryCount(1);
          lruEntryUpdate(newRe);
        } finally {
          OffHeapHelper.release(value);
          if (oldRe instanceof OffHeapRegionEntry) {
            ((OffHeapRegionEntry) oldRe).release();
          }
        }
        lruUpdateCallback();
      }
    } else {
      for (Iterator<RegionEntry> iter = regionEntries().iterator(); iter.hasNext();) {
        RegionEntry re = iter.next();
        if (re.isTombstone()) {
          if (re.getVersionStamp() == null) { // bug #50992 - recovery from versioned to
                                              // non-versioned
            iter.remove();
            continue;
          } else {
            tombstones.put(re.getVersionStamp().asVersionTag(), re);
          }
        } else {
          _getOwner().updateSizeOnCreate(re.getKey(),
              _getOwner().calculateRegionEntryValueSize(re));
          if (_getOwner() instanceof BucketRegionQueue) {
            BucketRegionQueue brq = (BucketRegionQueue) _getOwner();
            brq.incSecondaryQueueSize(1);
          }
        }
      }
      incEntryCount(size());
      // Since lru was not being done during recovery call it now.
      lruUpdateCallback();
    }

    // Schedule all of the tombstones, now that we have sorted them
    Map.Entry<VersionTag, RegionEntry> entry;
    while ((entry = tombstones.take()) != null) {
      // refresh the tombstone so it doesn't time out too soon
      _getOwner().scheduleTombstone(entry.getValue(), entry.getKey());
    }

  }

  private void copyRecoveredEntry(RegionEntry oldRe, RegionEntry newRe) {
    if (newRe.getVersionStamp() != null) {
      newRe.getVersionStamp().setMemberID(oldRe.getVersionStamp().getMemberID());
      newRe.getVersionStamp().setVersions(oldRe.getVersionStamp().asVersionTag());
    }

    if (newRe instanceof AbstractOplogDiskRegionEntry) {
      ((AbstractOplogDiskRegionEntry) newRe).setDiskId(oldRe);
      _getOwner().getDiskRegion().replaceIncompatibleEntry((DiskEntry) oldRe, (DiskEntry) newRe);
    }
    getEntryMap().put(newRe.getKey(), newRe);
  }

  @Override
  @Retained // Region entry may contain an off-heap value
  public RegionEntry initRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    boolean needsCallback = false;
    @Retained
    RegionEntry newRe =
        getEntryFactory().createEntry((RegionEntryContext) _getOwnerObject(), key, value);
    synchronized (newRe) {
      if (value.getVersionTag() != null && newRe.getVersionStamp() != null) {
        newRe.getVersionStamp().setVersions(value.getVersionTag());
      }
      RegionEntry oldRe = putEntryIfAbsent(key, newRe);
      while (oldRe != null) {
        synchronized (oldRe) {
          if (oldRe.isRemoved() && !oldRe.isTombstone()) {
            if (_isOwnerALocalRegion()) {
              _getOwner().getCachePerfStats().incRetries();
            }
            getEntryMap().remove(key, oldRe);
            oldRe = putEntryIfAbsent(key, newRe);
          }
          /*
           * Entry already exists which should be impossible. Free the current entry (if off-heap)
           * and throw an exception.
           */
          else {
            if (newRe instanceof OffHeapRegionEntry) {
              ((OffHeapRegionEntry) newRe).release();
            }

            throw new IllegalStateException(
                "Could not recover entry for key " + key + ".  The entry already exists!");
          }
        } // synchronized
      }
      if (_isOwnerALocalRegion()) {
        if (newRe.isTombstone()) {
          // refresh the tombstone so it doesn't time out too soon
          _getOwner().scheduleTombstone(newRe, newRe.getVersionStamp().asVersionTag());
        } else {
          _getOwner().updateSizeOnCreate(key, _getOwner().calculateRegionEntryValueSize(newRe));
        }
        // incEntryCount is called for a tombstone because scheduleTombstone does entryCount--.
        incEntryCount(1); // we are creating an entry that was recovered from disk including
                          // tombstone
      }
      lruEntryUpdate(newRe);
      needsCallback = true;
    }
    if (needsCallback) {
      lruUpdateCallback();
    }

    EntryLogger.logRecovery(_getOwnerObject(), key, value);

    return newRe;
  }

  @Override
  public RegionEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    boolean needsCallback = false;
    RegionEntry re = getEntry(key);
    if (re == null) {
      return null;
    }
    synchronized (re) {
      if (re.isRemoved() && !re.isTombstone()) {
        return null;
      }
      if (value.getVersionTag() != null && re.getVersionStamp() != null) {
        re.getVersionStamp().setVersions(value.getVersionTag());
      }
      try {
        if (_isOwnerALocalRegion()) {
          boolean oldValueWasTombstone = re.isTombstone();
          if (oldValueWasTombstone) {
            // when a tombstone is to be overwritten, unschedule it first
            _getOwner().unscheduleTombstone(re);
            // unscheduleTombstone incs entryCount which is ok
            // because we either set the value after this so that
            // the entry exists or we call scheduleTombstone which
            // will dec entryCount.
          }
          final int oldSize = _getOwner().calculateRegionEntryValueSize(re);
          re.setValue(_getOwner(), value); // OFFHEAP no need to call
                                           // AbstractRegionMap.prepareValueForCache because
                                           // setValue is overridden for disk and that code takes
                                           // apart value (RecoveredEntry) and prepares its nested
                                           // value for the cache
          if (re.isTombstone()) {
            _getOwner().scheduleTombstone(re, re.getVersionStamp().asVersionTag());
            _getOwner().updateSizeOnRemove(key, oldSize);
          } else if (oldValueWasTombstone) {
            _getOwner().updateSizeOnCreate(key, _getOwner().calculateRegionEntryValueSize(re));
          } else {
            _getOwner().updateSizeOnPut(key, oldSize,
                _getOwner().calculateRegionEntryValueSize(re));
          }
        } else {
          value.applyToDiskEntry((PlaceHolderDiskRegion) _getOwnerObject(), (DiskEntry) re,
              (RegionEntryContext) _getOwnerObject());
        }
      } catch (RegionClearedException rce) {
        throw new IllegalStateException(
            "RegionClearedException should never happen in this context", rce);
      }
      lruEntryUpdate(re);
      needsCallback = true;
    }
    if (needsCallback) {
      lruUpdateCallback();
    }

    EntryLogger.logRecovery(_getOwnerObject(), key, value);

    return re;
  }

  @Override
  public boolean initialImagePut(final Object key, final long lastModified, Object newValue,
      final boolean wasRecovered, boolean deferLRUCallback, VersionTag entryVersion,
      InternalDistributedMember sender, boolean isSynchronizing) {
    boolean result = false;
    boolean done = false;
    boolean cleared = false;
    final LocalRegion owner = _getOwner();

    if (newValue == Token.TOMBSTONE && !owner.getConcurrencyChecksEnabled()) {
      return false;
    }

    if (owner instanceof HARegion && newValue instanceof CachedDeserializable) {
      newValue = ((HARegion) owner).updateHAEventWrapper(sender, (CachedDeserializable) newValue);
      if (newValue == null) {
        return false;
      }
    }

    try {
      RegionEntry newRe = getEntryFactory().createEntry(owner, key, Token.REMOVED_PHASE1);
      EntryEventImpl event = null;

      @Retained
      @Released
      Object oldValue = null;

      try {
        RegionEntry oldRe = null;
        synchronized (newRe) {
          try {
            oldRe = putEntryIfAbsent(key, newRe);
            while (!done && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  owner.getCachePerfStats().incRetries();
                  getEntryMap().remove(key, oldRe);
                  oldRe = putEntryIfAbsent(key, newRe);
                } else {
                  boolean acceptedVersionTag = false;
                  if (entryVersion != null && owner.getConcurrencyChecksEnabled()) {
                    Assert.assertTrue(entryVersion.getMemberID() != null,
                        "GII entry versions must have identifiers");
                    try {
                      boolean isTombstone = (newValue == Token.TOMBSTONE);
                      // don't reschedule the tombstone if it hasn't changed
                      boolean isSameTombstone = oldRe.isTombstone() && isTombstone
                          && oldRe.getVersionStamp().asVersionTag().equals(entryVersion);
                      if (isSameTombstone) {
                        return true;
                      }
                      processVersionTagForGII(oldRe, owner, entryVersion, isTombstone, sender,
                          !wasRecovered || isSynchronizing);
                      acceptedVersionTag = true;
                    } catch (ConcurrentCacheModificationException e) {
                      return false;
                    }
                  }
                  final boolean oldIsTombstone = oldRe.isTombstone();
                  final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                  try {
                    result = oldRe.initialImagePut(owner, lastModified, newValue, wasRecovered,
                        acceptedVersionTag);
                    if (result) {
                      if (oldIsTombstone) {
                        owner.unscheduleTombstone(oldRe);
                        if (newValue != Token.TOMBSTONE) {
                          lruEntryCreate(oldRe);
                        } else {
                          lruEntryUpdate(oldRe);
                        }
                      }
                      if (newValue == Token.TOMBSTONE) {
                        owner.updateSizeOnRemove(key, oldSize);
                        if (owner.getServerProxy() == null
                            && owner.getVersionVector().isTombstoneTooOld(
                                entryVersion.getMemberID(), entryVersion.getRegionVersion())) {
                          // the received tombstone has already been reaped, so don't retain it
                          removeTombstone(oldRe, entryVersion, false, false);
                          return false;
                        } else {
                          owner.scheduleTombstone(oldRe, entryVersion);
                          lruEntryDestroy(oldRe);
                        }
                      } else {
                        int newSize = owner.calculateRegionEntryValueSize(oldRe);
                        if (!oldIsTombstone) {
                          owner.updateSizeOnPut(key, oldSize, newSize);
                        } else {
                          owner.updateSizeOnCreate(key, newSize);
                        }
                        EntryLogger.logInitialImagePut(_getOwnerObject(), key, newValue);
                      }
                    }
                    if (owner.getIndexManager() != null) {
                      // Due to having no reverse map, we need to be able to generate the oldkey
                      // before doing an update
                      // Without the BEFORE_UPDATE_OP, we would see duplicate entries in the index
                      // as the update could not locate the old key
                      if (!oldRe.isRemoved()) {
                        owner.getIndexManager().updateIndexes(oldRe, IndexManager.REMOVE_ENTRY,
                            IndexProtocol.BEFORE_UPDATE_OP);
                      }
                      owner.getIndexManager().updateIndexes(oldRe,
                          oldRe.isRemoved() ? IndexManager.ADD_ENTRY : IndexManager.UPDATE_ENTRY,
                          oldRe.isRemoved() ? IndexProtocol.OTHER_OP
                              : IndexProtocol.AFTER_UPDATE_OP);
                    }
                    done = true;
                  } finally {
                    if (event != null) {
                      event.release();
                      event = null;
                    }
                  }
                }
              }
            }
            if (!done) {
              boolean versionTagAccepted = false;
              if (entryVersion != null && owner.getConcurrencyChecksEnabled()) {
                Assert.assertTrue(entryVersion.getMemberID() != null,
                    "GII entry versions must have identifiers");
                try {
                  boolean isTombstone = (newValue == Token.TOMBSTONE);
                  processVersionTagForGII(newRe, owner, entryVersion, isTombstone, sender,
                      !wasRecovered || isSynchronizing);
                  versionTagAccepted = true;
                } catch (ConcurrentCacheModificationException e) {
                  return false;
                }
              }
              result = newRe.initialImageInit(owner, lastModified, newValue, true, wasRecovered,
                  versionTagAccepted);
              try {
                if (result) {
                  if (newValue == Token.TOMBSTONE) {
                    owner.scheduleTombstone(newRe, entryVersion);
                  } else {
                    owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(newRe));
                    EntryLogger.logInitialImagePut(_getOwnerObject(), key, newValue);
                    lruEntryCreate(newRe);
                  }
                  incEntryCount(1);
                }

                // Update local indexes
                if (owner.getIndexManager() != null) {
                  // Due to having no reverse map, we need to be able to generate the oldkey before
                  // doing an update
                  // Without the BEFORE_UPDATE_OP, we would see duplicate entries in the index as
                  // the update could not locate the old key
                  if (oldRe != null && !oldRe.isRemoved()) {
                    owner.getIndexManager().updateIndexes(oldRe, IndexManager.REMOVE_ENTRY,
                        IndexProtocol.BEFORE_UPDATE_OP);
                  }
                  owner.getIndexManager().updateIndexes(newRe,
                      newRe.isRemoved() ? IndexManager.REMOVE_ENTRY : IndexManager.UPDATE_ENTRY,
                      newRe.isRemoved() ? IndexProtocol.OTHER_OP : IndexProtocol.AFTER_UPDATE_OP);
                }
                done = true;
              } finally {
                if (event != null) {
                  event.release();
                  event = null;
                }
              }
            }
          } finally {
            if (done && result) {
              initialImagePutEntry(newRe);
              if (owner instanceof BucketRegionQueue) {
                BucketRegionQueue brq = (BucketRegionQueue) owner;
                brq.addToEventQueue(key, done, event);
              }
            }
            if (!done) {
              removeEntry(key, newRe, false);
              if (owner.getIndexManager() != null) {
                owner.getIndexManager().updateIndexes(newRe, IndexManager.REMOVE_ENTRY,
                    IndexProtocol.OTHER_OP);
              }
            }
          }
        } // synchronized
      } finally {
        if (event != null)
          event.release();
        OffHeapHelper.release(oldValue);
      }
    } catch (RegionClearedException rce) {
      // Asif: do not issue any sort of callbacks
      done = false;
      cleared = true;
    } catch (QueryException qe) {
      done = false;
      cleared = true;
    } finally {
      if (done && !deferLRUCallback) {
        lruUpdateCallback();
      } else if (!cleared) {
        resetThreadLocals();
      }
    }
    return result;
  }

  private void initialImagePutEntry(RegionEntry newRe) {}

  @Override
  public boolean confirmEvictionDestroy(RegionEntry regionEntry) {
    /* We arn't in an LRU context, and should never get here */
    Assert.assertTrue(false, "Not an LRU region, can not confirm LRU eviction operation");
    return true;
  }

  @Override
  public boolean destroy(EntryEventImpl event, boolean inTokenMode, boolean duringRI,
      boolean cacheWrite, boolean isEviction, Object expectedOldValue, boolean removeRecoveredEntry)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    RegionMapDestroy regionMapDestroy = new RegionMapDestroy((InternalRegion) owner, this, this);
    return regionMapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);
  }

  @Override
  public void txApplyDestroy(Object key, TransactionId txId, TXRmtEvent txEvent,
      boolean inTokenMode, boolean inRI, Operation op, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, boolean isOriginRemote, TXEntryState txEntryState,
      VersionTag versionTag, long tailKey) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final LocalRegion owner = _getOwner();

    final boolean isRegionReady = !inTokenMode;
    final boolean hasRemoteOrigin = !((TXId) txId).getMemberId().equals(owner.getMyId());
    boolean cbEventInPending = false;
    IndexManager oqlIndexManager = owner.getIndexManager();
    try {
      RegionEntry re = getEntry(key);
      if (re != null) {
        // Fix for Bug #44431. We do NOT want to update the region and wait
        // later for index INIT as region.clear() can cause inconsistency if
        // happened in parallel as it also does index INIT.
        if (oqlIndexManager != null) {
          oqlIndexManager.waitForIndexInit();
        }
        try {
          synchronized (re) {
            if (!re.isRemoved() || re.isTombstone()) {
              Object oldValue = re.getValueInVM(owner);
              final int oldSize = owner.calculateRegionEntryValueSize(re);
              // Create an entry event only if the calling context is
              // a receipt of a TXCommitMessage AND there are callbacks installed
              // for this region
              boolean invokeCallbacks = shouldCreateCBEvent(owner, isRegionReady || inRI);
              @Released
              EntryEventImpl cbEvent =
                  createCBEvent(owner, op, key, null, txId, txEvent, eventId, aCallbackArgument,
                      filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
              try {

                if (owner.isUsedForPartitionedRegionBucket()) {
                  txHandleWANEvent(owner, cbEvent, txEntryState);
                }
                cbEvent.setRegionEntry(re);
                cbEvent.setOldValue(oldValue);
                if (isDebugEnabled) {
                  logger.debug("txApplyDestroy cbEvent={}", cbEvent);
                }

                txRemoveOldIndexEntry(Operation.DESTROY, re);
                if (txEvent != null) {
                  txEvent.addDestroy(owner, re, re.getKey(), aCallbackArgument);
                }
                boolean clearOccured = false;
                try {
                  processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
                  if (inTokenMode) {
                    if (oldValue == Token.TOMBSTONE) {
                      owner.unscheduleTombstone(re);
                    }
                    re.setValue(owner, Token.DESTROYED);
                  } else {
                    if (!re.isTombstone()) {
                      {
                        if (shouldPerformConcurrencyChecks(owner, cbEvent)
                            && cbEvent.getVersionTag() != null) {
                          re.makeTombstone(owner, cbEvent.getVersionTag());
                        } else {
                          re.removePhase1(owner, false); // fix for bug 43063
                          re.removePhase2();
                          removeEntry(key, re, false);
                        }
                      }
                    } else {
                      owner.rescheduleTombstone(re, re.getVersionStamp().asVersionTag());
                    }
                  }
                  EntryLogger.logTXDestroy(_getOwnerObject(), key);
                  owner.updateSizeOnRemove(key, oldSize);
                } catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                owner.txApplyDestroyPart2(re, re.getKey(), inTokenMode,
                    clearOccured /* Clear Conflciting with the operation */);
                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if (pendingCallbacks == null) {
                    owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, cbEvent,
                        true/* callDispatchListenerEvent */);
                  } else {
                    pendingCallbacks.add(cbEvent);
                    cbEventInPending = true;
                  }
                }
                if (!clearOccured) {
                  lruEntryDestroy(re);
                }
                if (owner.getConcurrencyChecksEnabled() && txEntryState != null
                    && cbEvent != null) {
                  txEntryState.setVersionTag(cbEvent.getVersionTag());
                }
              } finally {
                if (!cbEventInPending)
                  cbEvent.release();
              }
            }
          }
        } finally {
          if (oqlIndexManager != null) {
            oqlIndexManager.countDownIndexUpdaters();
          }
        }
      } else if (inTokenMode || owner.getConcurrencyChecksEnabled()) {
        // treating tokenMode and re == null as same, since we now want to
        // generate versions and Tombstones for destroys
        boolean dispatchListenerEvent = inTokenMode;
        boolean opCompleted = false;
        // TODO: if inTokenMode then Token.DESTROYED is ok but what about !inTokenMode because
        // owner.concurrencyChecksEnabled? In that case we do not want a DESTROYED token.
        RegionEntry newRe = getEntryFactory().createEntry(owner, key, Token.DESTROYED);
        if (oqlIndexManager != null) {
          oqlIndexManager.waitForIndexInit();
        }
        EntryEventImpl cbEvent = null;
        try {
          synchronized (newRe) {
            RegionEntry oldRe = putEntryIfAbsent(key, newRe);
            while (!opCompleted && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  owner.getCachePerfStats().incRetries();
                  getEntryMap().remove(key, oldRe);
                  oldRe = putEntryIfAbsent(key, newRe);
                } else {
                  try {
                    boolean invokeCallbacks = shouldCreateCBEvent(owner, isRegionReady || inRI);
                    cbEvent = createCBEvent(owner, op, key, null, txId, txEvent, eventId,
                        aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState,
                        versionTag, tailKey);
                    try {
                      cbEvent.setRegionEntry(oldRe);
                      cbEvent.setOldValue(Token.NOT_AVAILABLE);
                      if (isDebugEnabled) {
                        logger.debug("txApplyDestroy token mode cbEvent={}", cbEvent);
                      }
                      if (owner.isUsedForPartitionedRegionBucket()) {
                        txHandleWANEvent(owner, cbEvent, txEntryState);
                      }
                      processAndGenerateTXVersionTag(owner, cbEvent, oldRe, txEntryState);
                      if (invokeCallbacks) {
                        switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                        if (pendingCallbacks == null) {
                          owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, cbEvent,
                              dispatchListenerEvent);
                        } else {
                          pendingCallbacks.add(cbEvent);
                          cbEventInPending = true;
                        }
                      }
                      int oldSize = 0;
                      boolean wasTombstone = oldRe.isTombstone();
                      {
                        if (!wasTombstone) {
                          oldSize = owner.calculateRegionEntryValueSize(oldRe);
                        }
                      }
                      oldRe.setValue(owner, Token.DESTROYED);
                      EntryLogger.logTXDestroy(_getOwnerObject(), key);
                      if (wasTombstone) {
                        owner.unscheduleTombstone(oldRe);
                      }
                      owner.updateSizeOnRemove(oldRe.getKey(), oldSize);
                      owner.txApplyDestroyPart2(oldRe, oldRe.getKey(), inTokenMode,
                          false /* Clear Conflicting with the operation */);
                      lruEntryDestroy(oldRe);
                    } finally {
                      if (!cbEventInPending)
                        cbEvent.release();
                    }
                  } catch (RegionClearedException rce) {
                    owner.txApplyDestroyPart2(oldRe, oldRe.getKey(), inTokenMode,
                        true /* Clear Conflicting with the operation */);
                  }
                  if (shouldPerformConcurrencyChecks(owner, cbEvent)
                      && cbEvent.getVersionTag() != null) {
                    oldRe.makeTombstone(owner, cbEvent.getVersionTag());
                  } else if (!inTokenMode) {
                    // only remove for NORMAL regions if they do not generate versions see 51781
                    oldRe.removePhase1(owner, false); // fix for bug 43063
                    oldRe.removePhase2();
                    removeEntry(key, oldRe, false);
                  }
                  opCompleted = true;
                }
              }
            }
            if (!opCompleted) {
              // already has value set to Token.DESTROYED
              opCompleted = true;
              boolean invokeCallbacks = shouldCreateCBEvent(owner, isRegionReady || inRI);
              cbEvent =
                  createCBEvent(owner, op, key, null, txId, txEvent, eventId, aCallbackArgument,
                      filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
              try {
                cbEvent.setRegionEntry(newRe);
                cbEvent.setOldValue(Token.NOT_AVAILABLE);
                if (isDebugEnabled) {
                  logger.debug("txApplyDestroy token mode cbEvent={}", cbEvent);
                }
                if (owner.isUsedForPartitionedRegionBucket()) {
                  txHandleWANEvent(owner, cbEvent, txEntryState);
                }
                processAndGenerateTXVersionTag(owner, cbEvent, newRe, txEntryState);
                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if (pendingCallbacks == null) {
                    owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, cbEvent,
                        dispatchListenerEvent);
                  } else {
                    pendingCallbacks.add(cbEvent);
                    cbEventInPending = true;
                  }
                }
                EntryLogger.logTXDestroy(_getOwnerObject(), key);
                owner.updateSizeOnCreate(newRe.getKey(), 0);
                if (shouldPerformConcurrencyChecks(owner, cbEvent)
                    && cbEvent.getVersionTag() != null) {
                  newRe.makeTombstone(owner, cbEvent.getVersionTag());
                } else if (!inTokenMode) {
                  // only remove for NORMAL regions if they do not generate versions see 51781
                  newRe.removePhase1(owner, false); // fix for bug 43063
                  newRe.removePhase2();
                  removeEntry(key, newRe, false);
                }
                owner.txApplyDestroyPart2(newRe, newRe.getKey(), inTokenMode,
                    false /* clearConflict */);
                // Note no need for LRU work since the entry is destroyed
                // and will be removed when gii completes
              } finally {
                if (!cbEventInPending)
                  cbEvent.release();
              }
            }
            if (owner.getConcurrencyChecksEnabled() && txEntryState != null && cbEvent != null) {
              txEntryState.setVersionTag(cbEvent.getVersionTag());
            }
          }
        } catch (RegionClearedException e) {
          // TODO
        } finally {
          if (oqlIndexManager != null) {
            oqlIndexManager.countDownIndexUpdaters();
          }
        }
      } else if (re == null) {
        // Fix bug#43594
        // In cases where bucket region is re-created, it may so happen that
        // the destroy is already applied on the Initial image provider, thus
        // causing region entry to be absent.
        // Notify clients with client events.
        @Released
        EntryEventImpl cbEvent = createCBEvent(owner, op, key, null, txId, txEvent, eventId,
            aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
        try {
          if (owner.isUsedForPartitionedRegionBucket()) {
            txHandleWANEvent(owner, cbEvent, txEntryState);
          }
          switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
          if (pendingCallbacks == null) {
            owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, cbEvent, false);
          } else {
            pendingCallbacks.add(cbEvent);
            cbEventInPending = true;
          }
        } finally {
          if (!cbEventInPending)
            cbEvent.release();
        }
      }
    } catch (DiskAccessException dae) {
      owner.handleDiskAccessException(dae);
      throw dae;
    }
  }

  /**
   * If true then invalidates that throw EntryNotFoundException or that are already invalid will
   * first call afterInvalidate on CacheListeners. The old value on the event passed to
   * afterInvalidate will be null. If the region is not initialized then callbacks will not be done.
   * This property only applies to non-transactional invalidates. Transactional invalidates ignore
   * this property. Note that empty "proxy" regions on a client will not be sent invalidates from
   * the server unless they also set the proxy InterestPolicy to ALL. If the invalidate is not sent
   * then this property will not cause a listener on that client to be notified of the invalidate. A
   * non-empty "caching-proxy" will receive invalidates from the server.
   */
  public static boolean FORCE_INVALIDATE_EVENT =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "FORCE_INVALIDATE_EVENT");

  /**
   * If the FORCE_INVALIDATE_EVENT flag is true then invoke callbacks on the given event.
   */
  static void forceInvalidateEvent(EntryEventImpl event, LocalRegion owner) {
    if (FORCE_INVALIDATE_EVENT) {
      event.invokeCallbacks(owner, false, false);
    }
  }

  @Override
  public boolean invalidate(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry,
      boolean forceCallbacks) throws EntryNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this + " is null for event " + event);

    }
    boolean didInvalidate = false;
    RegionEntry invalidatedRe = null;
    boolean clearOccured = false;
    DiskRegion dr = owner.getDiskRegion();
    boolean ownerIsInitialized = owner.isInitialized();
    try {
      // Fix for Bug #44431. We do NOT want to update the region and wait
      // later for index INIT as region.clear() can cause inconsistency if
      // happened in parallel as it also does index INIT.
      IndexManager oqlIndexManager = owner.getIndexManager();
      if (oqlIndexManager != null) {
        oqlIndexManager.waitForIndexInit();
      }
      lockForCacheModification(owner, event);
      try {
        try {
          if (forceNewEntry || forceCallbacks) {
            boolean opCompleted = false;
            RegionEntry newRe =
                getEntryFactory().createEntry(owner, event.getKey(), Token.REMOVED_PHASE1);
            synchronized (newRe) {
              try {
                RegionEntry oldRe = putEntryIfAbsent(event.getKey(), newRe);

                while (!opCompleted && oldRe != null) {
                  synchronized (oldRe) {
                    // if the RE is in phase 2 of removal, it will really be removed
                    // from the map. Otherwise, we can use it here and the thread
                    // that is destroying the RE will see the invalidation and not
                    // proceed to phase 2 of removal.
                    if (oldRe.isRemovedPhase2()) {
                      owner.getCachePerfStats().incRetries();
                      getEntryMap().remove(event.getKey(), oldRe);
                      oldRe = putEntryIfAbsent(event.getKey(), newRe);
                    } else {
                      opCompleted = true;
                      event.setRegionEntry(oldRe);
                      if (oldRe.isDestroyed()) {
                        if (isDebugEnabled) {
                          logger.debug(
                              "mapInvalidate: Found DESTROYED token, not invalidated; key={}",
                              event.getKey());
                        }
                      } else if (oldRe.isInvalid()) {

                        // was already invalid, do not invoke listeners or increment stat
                        if (isDebugEnabled) {
                          logger.debug("mapInvalidate: Entry already invalid: '{}'",
                              event.getKey());
                        }
                        processVersionTag(oldRe, event);
                        try {
                          oldRe.setValue(owner, oldRe.getValueInVM(owner)); // OFFHEAP noop setting
                                                                            // an already invalid to
                                                                            // invalid; No need to
                                                                            // call
                                                                            // prepareValueForCache
                                                                            // since it is an
                                                                            // invalid token.
                        } catch (RegionClearedException e) {
                          // that's okay - when writing an invalid into a disk, the
                          // region has been cleared (including this token)
                        }
                      } else {
                        owner.serverInvalidate(event);
                        if (owner.getConcurrencyChecksEnabled()
                            && event.noVersionReceivedFromServer()) {
                          // server did not perform the invalidation, so don't leave an invalid
                          // entry here
                          return false;
                        }
                        final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                        // added for cq which needs old value. rdubey
                        FilterProfile fp = owner.getFilterProfile();
                        if (!oldRe.isRemoved() && (fp != null && fp.getCqCount() > 0)) {

                          Object oldValue = oldRe.getValueInVM(owner); // OFFHEAP EntryEventImpl
                                                                       // oldValue

                          // this will not fault in the value.
                          if (oldValue == Token.NOT_AVAILABLE) {
                            event.setOldValue(oldRe.getValueOnDiskOrBuffer(owner));
                          } else {
                            event.setOldValue(oldValue);
                          }
                        }
                        boolean isCreate = false;
                        try {
                          if (oldRe.isRemoved()) {
                            processVersionTag(oldRe, event);
                            event.putNewEntry(owner, oldRe);
                            EntryLogger.logInvalidate(event);
                            owner.recordEvent(event);
                            if (!oldRe.isTombstone()) {
                              owner.updateSizeOnPut(event.getKey(), oldSize,
                                  event.getNewValueBucketSize());
                            } else {
                              owner.updateSizeOnCreate(event.getKey(),
                                  event.getNewValueBucketSize());
                              isCreate = true;
                            }
                          } else {
                            processVersionTag(oldRe, event);
                            event.putExistingEntry(owner, oldRe);
                            EntryLogger.logInvalidate(event);
                            owner.recordEvent(event);
                            owner.updateSizeOnPut(event.getKey(), oldSize,
                                event.getNewValueBucketSize());
                          }
                        } catch (RegionClearedException e) {
                          // generate versionTag for the event
                          EntryLogger.logInvalidate(event);
                          owner.recordEvent(event);
                          clearOccured = true;
                        }
                        owner.basicInvalidatePart2(oldRe, event,
                            clearOccured /* conflict with clear */, invokeCallbacks);
                        if (!clearOccured) {
                          if (isCreate) {
                            lruEntryCreate(oldRe);
                          } else {
                            lruEntryUpdate(oldRe);
                          }
                        }
                        didInvalidate = true;
                        invalidatedRe = oldRe;
                      }
                    }
                  } // synchronized oldRe
                } // while oldRe exists

                if (!opCompleted) {
                  if (forceNewEntry && event.isFromServer()) {
                    // don't invoke listeners - we didn't force new entries for
                    // CCU invalidations before 7.0, and listeners don't care
                    if (!FORCE_INVALIDATE_EVENT) {
                      event.inhibitCacheListenerNotification(true);
                    }
                  }
                  event.setRegionEntry(newRe);
                  owner.serverInvalidate(event);
                  if (!forceNewEntry && event.noVersionReceivedFromServer()) {
                    // server did not perform the invalidation, so don't leave an invalid
                    // entry here
                    return false;
                  }
                  try {
                    ownerIsInitialized = owner.isInitialized();
                    if (!ownerIsInitialized && owner.getDataPolicy().withReplication()) {
                      final int oldSize = owner.calculateRegionEntryValueSize(newRe);
                      invalidateEntry(event, newRe, oldSize);
                    } else {
                      invalidateNewEntry(event, owner, newRe);
                    }
                  } catch (RegionClearedException e) {
                    // TODO: deltaGII: do we even need RegionClearedException?
                    // generate versionTag for the event
                    owner.recordEvent(event);
                    clearOccured = true;
                  }
                  owner.basicInvalidatePart2(newRe, event, clearOccured /* conflict with clear */,
                      invokeCallbacks);
                  if (!clearOccured) {
                    lruEntryCreate(newRe);
                    incEntryCount(1);
                  }
                  opCompleted = true;
                  didInvalidate = true;
                  invalidatedRe = newRe;
                  // Don't leave an entry in the cache, if we
                  // just wanted to force the distribution and events
                  // for this invalidate
                  if (!forceNewEntry) {
                    removeEntry(event.getKey(), newRe, false);
                  }
                } // !opCompleted
              } catch (ConcurrentCacheModificationException ccme) {
                VersionTag tag = event.getVersionTag();
                if (tag != null && tag.isTimeStampUpdated()) {
                  // Notify gateways of new time-stamp.
                  owner.notifyTimestampsToGateways(event);
                }
                throw ccme;
              } finally {
                if (!opCompleted) {
                  removeEntry(event.getKey(), newRe, false);
                }
              }
            } // synchronized newRe
          } // forceNewEntry
          else { // !forceNewEntry
            boolean retry = true;
            // RegionEntry retryEntry = null;
            // int retries = -1;

            while (retry) {
              retry = false;
              boolean entryExisted = false;
              RegionEntry re = getEntry(event.getKey());
              RegionEntry tombstone = null;
              boolean haveTombstone = false;
              if (re != null && re.isTombstone()) {
                tombstone = re;
                haveTombstone = true;
                re = null;
              }
              if (re == null) {
                ownerIsInitialized = owner.isInitialized();
                if (!ownerIsInitialized) {
                  // when GII message arrived or processed later than invalidate
                  // message, the entry should be created as placeholder
                  RegionEntry newRe = haveTombstone ? tombstone
                      : getEntryFactory().createEntry(owner, event.getKey(), Token.INVALID);
                  synchronized (newRe) {
                    if (haveTombstone && !tombstone.isTombstone()) {
                      // state of the tombstone has changed so we need to retry
                      retry = true;
                      // retryEntry = tombstone; // leave this in place for debugging
                      continue;
                    }
                    re = putEntryIfAbsent(event.getKey(), newRe);
                    if (re == tombstone) {
                      re = null; // pretend we don't have an entry
                    }
                  }
                } else if (owner.getServerProxy() != null) {
                  Object sync = haveTombstone ? tombstone : new Object();
                  synchronized (sync) {
                    if (haveTombstone && !tombstone.isTombstone()) {
                      // bug 45295: state of the tombstone has changed so we need to retry
                      retry = true;
                      // retryEntry = tombstone; // leave this in place for debugging
                      continue;
                    }

                    // bug #43287 - send event to server even if it's not in the client (LRU may
                    // have evicted it)
                    owner.serverInvalidate(event);
                    if (owner.getConcurrencyChecksEnabled()) {
                      if (event.getVersionTag() == null) {
                        // server did not perform the invalidation, so don't leave an invalid
                        // entry here
                        return false;
                      } else if (tombstone != null) {
                        processVersionTag(tombstone, event);
                        try {
                          if (!tombstone.isTombstone()) {
                            if (isDebugEnabled) {
                              logger.debug("tombstone is no longer a tombstone. {}:event={}",
                                  tombstone, event);
                            }
                          }
                          tombstone.setValue(owner, Token.TOMBSTONE);
                        } catch (RegionClearedException e) {
                          // that's okay - when writing a tombstone into a disk, the
                          // region has been cleared (including this tombstone)
                        } catch (ConcurrentCacheModificationException ccme) {
                          VersionTag tag = event.getVersionTag();
                          if (tag != null && tag.isTimeStampUpdated()) {
                            // Notify gateways of new time-stamp.
                            owner.notifyTimestampsToGateways(event);
                          }
                          throw ccme;
                        }
                        // update the tombstone's version to prevent an older CCU/putAll from
                        // overwriting it
                        owner.rescheduleTombstone(tombstone, event.getVersionTag());
                      }
                    }
                  }
                  entryExisted = true;
                }
              }
              if (re != null) {
                // Gester: Race condition in GII
                // when adding the placeholder for invalidate entry during GII,
                // if the GII got processed earlier for this entry, then do
                // normal invalidate operation
                synchronized (re) {
                  if (!event.isOriginRemote() && event.getOperation().isExpiration()) {
                    // If this expiration started locally then only do it if the RE is not being
                    // used by a tx.
                    if (re.isInUseByTransaction()) {
                      return false;
                    }
                  }
                  if (re.isTombstone() || (!re.isRemoved() && !re.isDestroyed())) {
                    entryExisted = true;
                    if (re.isInvalid()) {
                      // was already invalid, do not invoke listeners or increment
                      // stat
                      if (isDebugEnabled) {
                        logger.debug("Invalidate: Entry already invalid: '{}'", event.getKey());
                      }
                      if (event.getVersionTag() != null && owner.getVersionVector() != null) {
                        owner.getVersionVector().recordVersion(
                            (InternalDistributedMember) event.getDistributedMember(),
                            event.getVersionTag());
                      }
                    } else { // previous value not invalid
                      event.setRegionEntry(re);
                      owner.serverInvalidate(event);
                      if (owner.getConcurrencyChecksEnabled()
                          && event.noVersionReceivedFromServer()) {
                        // server did not perform the invalidation, so don't leave an invalid
                        // entry here
                        if (isDebugEnabled) {
                          logger.debug(
                              "returning early because server did not generate a version stamp for this event:{}",
                              event);
                        }
                        return false;
                      }
                      // in case of overflow to disk we need the old value for cqs.
                      if (owner.getFilterProfile().getCqCount() > 0) {
                        // use to be getValue and can cause dead lock rdubey.
                        if (re.isValueNull()) {
                          event.setOldValue(re.getValueOnDiskOrBuffer(owner));
                        } else {
                          Object v = re.getValueInVM(owner);
                          event.setOldValue(v); // OFFHEAP escapes to EntryEventImpl oldValue
                        }
                      }
                      final boolean oldWasTombstone = re.isTombstone();
                      final int oldSize = _getOwner().calculateRegionEntryValueSize(re);
                      try {
                        invalidateEntry(event, re, oldSize);
                      } catch (RegionClearedException rce) {
                        // generate versionTag for the event
                        EntryLogger.logInvalidate(event);
                        _getOwner().recordEvent(event);
                        clearOccured = true;
                      } catch (ConcurrentCacheModificationException ccme) {
                        VersionTag tag = event.getVersionTag();
                        if (tag != null && tag.isTimeStampUpdated()) {
                          // Notify gateways of new time-stamp.
                          owner.notifyTimestampsToGateways(event);
                        }
                        throw ccme;
                      }
                      owner.basicInvalidatePart2(re, event, clearOccured /* conflict with clear */,
                          invokeCallbacks);
                      if (!clearOccured) {
                        if (oldWasTombstone) {
                          lruEntryCreate(re);
                        } else {
                          lruEntryUpdate(re);
                        }
                      }
                      didInvalidate = true;
                      invalidatedRe = re;
                    } // previous value not invalid
                  }
                } // synchronized re
              } // re != null
              else {
                // At this point, either it's not in GII mode, or the placeholder
                // is in region, do nothing
              }
              if (!entryExisted) {
                owner.checkEntryNotFound(event.getKey());
              }
            } // while(retry)
          } // !forceNewEntry
        } catch (DiskAccessException dae) {
          invalidatedRe = null;
          didInvalidate = false;
          this._getOwner().handleDiskAccessException(dae);
          throw dae;
        } finally {
          if (oqlIndexManager != null) {
            oqlIndexManager.countDownIndexUpdaters();
          }
          if (invalidatedRe != null) {
            owner.basicInvalidatePart3(invalidatedRe, event, invokeCallbacks);
          }
          if (didInvalidate && !clearOccured) {
            try {
              lruUpdateCallback();
            } catch (DiskAccessException dae) {
              this._getOwner().handleDiskAccessException(dae);
              throw dae;
            }
          } else if (!didInvalidate) {
            resetThreadLocals();
          }
        }
        return didInvalidate;
      } finally {
        if (ownerIsInitialized) {
          forceInvalidateEvent(event, owner);
        }
      }
    } finally {
      releaseCacheModificationLock(owner, event);
    }

  }

  private void invalidateNewEntry(EntryEventImpl event, final LocalRegion owner, RegionEntry newRe)
      throws RegionClearedException {
    processVersionTag(newRe, event);
    event.putNewEntry(owner, newRe);
    owner.recordEvent(event);
    owner.updateSizeOnCreate(event.getKey(), event.getNewValueBucketSize());
  }

  protected void invalidateEntry(EntryEventImpl event, RegionEntry re, int oldSize)
      throws RegionClearedException {
    processVersionTag(re, event);
    event.putExistingEntry(_getOwner(), re);
    EntryLogger.logInvalidate(event);
    _getOwner().recordEvent(event);
    _getOwner().updateSizeOnPut(event.getKey(), oldSize, event.getNewValueBucketSize());
  }


  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.RegionMap#updateEntryVersion(org.apache.geode.internal.cache.
   * EntryEventImpl)
   */
  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {

    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this + " is null for event " + event);

    }

    DiskRegion dr = owner.getDiskRegion();
    if (dr != null) {
      dr.setClearCountReference();
    }

    lockForCacheModification(owner, event);

    try {
      RegionEntry re = getEntry(event.getKey());

      boolean entryExisted = false;

      if (re != null) {
        // process version tag
        synchronized (re) {

          try {
            if (re.isTombstone() || (!re.isRemoved() && !re.isDestroyed())) {
              entryExisted = true;
            }
            processVersionTag(re, event);
            owner.generateAndSetVersionTag(event, re);
            EntryLogger.logUpdateEntryVersion(event);
            _getOwner().recordEvent(event);
          } catch (ConcurrentCacheModificationException ccme) {
            // Do nothing.
          }

        }
      }
      if (!entryExisted) {
        owner.checkEntryNotFound(event.getKey());
      }
    } catch (DiskAccessException dae) {
      this._getOwner().handleDiskAccessException(dae);
      throw dae;
    } finally {
      releaseCacheModificationLock(owner, event);
      if (dr != null) {
        dr.removeClearCountReference();
      }
    }
  }

  @Override
  public void txApplyInvalidate(Object key, Object newValue, boolean didDestroy, TransactionId txId,
      TXRmtEvent txEvent, boolean localOp, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    // boolean didInvalidate = false;
    final LocalRegion owner = _getOwner();

    @Released
    EntryEventImpl cbEvent = null;
    boolean forceNewEntry = !owner.isInitialized() && owner.isAllEvents();

    final boolean hasRemoteOrigin = !((TXId) txId).getMemberId().equals(owner.getMyId());
    DiskRegion dr = owner.getDiskRegion();
    // Fix for Bug #44431. We do NOT want to update the region and wait
    // later for index INIT as region.clear() can cause inconsistency if
    // happened in parallel as it also does index INIT.
    IndexManager oqlIndexManager = owner.getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    try {
      if (forceNewEntry) {
        boolean opCompleted = false;
        RegionEntry newRe = getEntryFactory().createEntry(owner, key, Token.REMOVED_PHASE1);
        synchronized (newRe) {
          try {
            RegionEntry oldRe = putEntryIfAbsent(key, newRe);
            while (!opCompleted && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  owner.getCachePerfStats().incRetries();
                  getEntryMap().remove(key, oldRe);
                  oldRe = putEntryIfAbsent(key, newRe);
                } else {
                  opCompleted = true;
                  final boolean oldWasTombstone = oldRe.isTombstone();
                  final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                  Object oldValue = oldRe.getValueInVM(owner); // OFFHEAP eei
                  // Create an entry event only if the calling context is
                  // a receipt of a TXCommitMessage AND there are callbacks
                  // installed
                  // for this region
                  boolean invokeCallbacks = shouldCreateCBEvent(owner, owner.isInitialized());
                  boolean cbEventInPending = false;
                  cbEvent = createCBEvent(owner,
                      localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, key, newValue,
                      txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext,
                      txEntryState, versionTag, tailKey);
                  try {
                    cbEvent.setRegionEntry(oldRe);
                    cbEvent.setOldValue(oldValue);
                    if (logger.isDebugEnabled()) {
                      logger.debug("txApplyInvalidate cbEvent={}", cbEvent);
                    }

                    txRemoveOldIndexEntry(Operation.INVALIDATE, oldRe);
                    if (didDestroy) {
                      oldRe.txDidDestroy(owner.cacheTimeMillis());
                    }
                    if (txEvent != null) {
                      txEvent.addInvalidate(owner, oldRe, oldRe.getKey(), newValue,
                          aCallbackArgument);
                    }
                    oldRe.setValueResultOfSearch(false);
                    processAndGenerateTXVersionTag(owner, cbEvent, oldRe, txEntryState);
                    boolean clearOccured = false;
                    try {
                      oldRe.setValue(owner, oldRe.prepareValueForCache(owner, newValue, true));
                      EntryLogger.logTXInvalidate(_getOwnerObject(), key);
                      owner.updateSizeOnPut(key, oldSize, 0);
                      if (oldWasTombstone) {
                        owner.unscheduleTombstone(oldRe);
                      }
                    } catch (RegionClearedException rce) {
                      clearOccured = true;
                    }
                    owner.txApplyInvalidatePart2(oldRe, oldRe.getKey(), didDestroy, true);
                    // didInvalidate = true;
                    if (invokeCallbacks) {
                      switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                      if (pendingCallbacks == null) {
                        owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                            true/* callDispatchListenerEvent */);
                      } else {
                        pendingCallbacks.add(cbEvent);
                        cbEventInPending = true;
                      }
                    }
                    if (!clearOccured) {
                      lruEntryUpdate(oldRe);
                    }
                    if (shouldPerformConcurrencyChecks(owner, cbEvent) && txEntryState != null) {
                      txEntryState.setVersionTag(cbEvent.getVersionTag());
                    }
                  } finally {
                    if (!cbEventInPending)
                      cbEvent.release();
                  }
                }
              }
            }
            if (!opCompleted) {
              boolean invokeCallbacks = shouldCreateCBEvent(owner, owner.isInitialized());
              boolean cbEventInPending = false;
              cbEvent =
                  createCBEvent(owner, localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE,
                      key, newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo,
                      bridgeContext, txEntryState, versionTag, tailKey);
              try {
                cbEvent.setRegionEntry(newRe);
                txRemoveOldIndexEntry(Operation.INVALIDATE, newRe);
                newRe.setValueResultOfSearch(false);
                boolean clearOccured = false;
                try {
                  processAndGenerateTXVersionTag(owner, cbEvent, newRe, txEntryState);
                  newRe.setValue(owner, newRe.prepareValueForCache(owner, newValue, true));
                  EntryLogger.logTXInvalidate(_getOwnerObject(), key);
                  owner.updateSizeOnCreate(newRe.getKey(), 0);// we are putting in a new invalidated
                                                              // entry
                } catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                owner.txApplyInvalidatePart2(newRe, newRe.getKey(), didDestroy, true);

                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if (pendingCallbacks == null) {
                    owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                        true/* callDispatchListenerEvent */);
                  } else {
                    pendingCallbacks.add(cbEvent);
                    cbEventInPending = true;
                  }
                }
                opCompleted = true;
                if (!clearOccured) {
                  lruEntryCreate(newRe);
                  incEntryCount(1);
                }
                if (shouldPerformConcurrencyChecks(owner, cbEvent) && txEntryState != null) {
                  txEntryState.setVersionTag(cbEvent.getVersionTag());
                }
              } finally {
                if (!cbEventInPending)
                  cbEvent.release();
              }
            }
          } finally {
            if (!opCompleted) {
              removeEntry(key, newRe, false);
            }
          }
        }
      } else { /* !forceNewEntry */
        RegionEntry re = getEntry(key);
        if (re != null) {
          synchronized (re) {
            // Fix GEODE-3204, do not invalidate the region entry if it is a removed token
            if (!Token.isRemoved(re.getValueAsToken())) {
              final int oldSize = owner.calculateRegionEntryValueSize(re);
              Object oldValue = re.getValueInVM(owner); // OFFHEAP eei
              // Create an entry event only if the calling context is
              // a receipt of a TXCommitMessage AND there are callbacks
              // installed
              // for this region
              boolean invokeCallbacks = shouldCreateCBEvent(owner, owner.isInitialized());
              boolean cbEventInPending = false;
              cbEvent =
                  createCBEvent(owner, localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE,
                      key, newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo,
                      bridgeContext, txEntryState, versionTag, tailKey);
              try {
                cbEvent.setRegionEntry(re);
                cbEvent.setOldValue(oldValue);
                txRemoveOldIndexEntry(Operation.INVALIDATE, re);
                if (didDestroy) {
                  re.txDidDestroy(owner.cacheTimeMillis());
                }
                if (txEvent != null) {
                  txEvent.addInvalidate(owner, re, re.getKey(), newValue, aCallbackArgument);
                }
                re.setValueResultOfSearch(false);
                processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
                boolean clearOccured = false;
                try {
                  re.setValue(owner, re.prepareValueForCache(owner, newValue, true));
                  EntryLogger.logTXInvalidate(_getOwnerObject(), key);
                  owner.updateSizeOnPut(key, oldSize, 0);
                } catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                owner.txApplyInvalidatePart2(re, re.getKey(), didDestroy, true);
                // didInvalidate = true;
                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if (pendingCallbacks == null) {
                    owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                        true/* callDispatchListenerEvent */);
                  } else {
                    pendingCallbacks.add(cbEvent);
                    cbEventInPending = true;
                  }
                }
                if (!clearOccured) {
                  lruEntryUpdate(re);
                }
                if (shouldPerformConcurrencyChecks(owner, cbEvent) && txEntryState != null) {
                  txEntryState.setVersionTag(cbEvent.getVersionTag());
                }
              } finally {
                if (!cbEventInPending)
                  cbEvent.release();
              }
              return;
            }
          }
        }
        { // re == null or region entry is removed token.
          // Fix bug#43594
          // In cases where bucket region is re-created, it may so happen
          // that the invalidate is already applied on the Initial image
          // provider, thus causing region entry to be absent.
          // Notify clients with client events.
          boolean cbEventInPending = false;
          cbEvent =
              createCBEvent(owner, localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, key,
                  newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo,
                  bridgeContext, txEntryState, versionTag, tailKey);
          try {
            switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
            if (pendingCallbacks == null) {
              owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, cbEvent, false);
            } else {
              pendingCallbacks.add(cbEvent);
              cbEventInPending = true;
            }
          } finally {
            if (!cbEventInPending)
              cbEvent.release();
          }
        }
      }
    } catch (DiskAccessException dae) {
      owner.handleDiskAccessException(dae);
      throw dae;
    } finally {
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
    }
  }

  /**
   * This code needs to be evaluated. It was added quickly to help PR persistence not to consume as
   * much memory.
   */
  @Override
  public void evictValue(Object key) {
    final LocalRegion owner = _getOwner();
    RegionEntry re = getEntry(key);
    if (re != null) {
      synchronized (re) {
        if (!re.isValueNull()) {
          re.setValueToNull();
          owner.getDiskRegion().incNumEntriesInVM(-1L);
          owner.getDiskRegion().incNumOverflowOnDisk(1L);
          if (owner instanceof BucketRegion) {
            ((BucketRegion) owner).incNumEntriesInVM(-1L);
            ((BucketRegion) owner).incNumOverflowOnDisk(1L);
          }
        }
      }
    }
  }

  /*
   * returns null if the operation fails
   */
  @Override
  public RegionEntry basicPut(EntryEventImpl event, final long unused, final boolean ifNew,
      final boolean ifOld, final Object expectedOldValue, // only non-null if ifOld
      final boolean requireOldValue, final boolean overwriteDestroyed)
      throws CacheWriterException, TimeoutException {

    final RegionMapPut regionMapPut =
        new RegionMapPut(this, _getOwner(), this, entryEventSerialization, event, ifNew, ifOld,
            overwriteDestroyed, requireOldValue, expectedOldValue);

    return regionMapPut.put();
  }

  @Override
  public void runWhileEvictionDisabled(Runnable r) {
    final boolean disabled = disableLruUpdateCallback();
    try {
      r.run();
    } finally {
      if (disabled) {
        enableLruUpdateCallback();
      }
    }
  }

  @Override
  public void txApplyPut(Operation p_putOp, Object key, Object nv, boolean didDestroy,
      TransactionId txId, TXRmtEvent txEvent, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this + " is null");
    }

    Operation putOp = p_putOp;

    Object newValue = nv;

    final boolean hasRemoteOrigin = !((TXId) txId).getMemberId().equals(owner.getMyId());
    final boolean isTXHost = txEntryState != null;
    final boolean isClientTXOriginator = owner.getCache().isClient() && !hasRemoteOrigin;
    final boolean isRegionReady = owner.isInitialized();
    @Released
    EntryEventImpl cbEvent = null;
    boolean invokeCallbacks = shouldCreateCBEvent(owner, isRegionReady);
    boolean cbEventInPending = false;
    cbEvent = createCallBackEvent(owner, putOp, key, newValue, txId, txEvent, eventId,
        aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("txApplyPut cbEvent={}", cbEvent);
      }

      if (owner.isUsedForPartitionedRegionBucket()) {
        newValue = EntryEventImpl.getCachedDeserializable(nv, cbEvent);
        txHandleWANEvent(owner, cbEvent, txEntryState);
      }

      boolean opCompleted = false;
      // Fix for Bug #44431. We do NOT want to update the region and wait
      // later for index INIT as region.clear() can cause inconsistency if
      // happened in parallel as it also does index INIT.
      IndexManager oqlIndexManager = owner.getIndexManager();
      if (oqlIndexManager != null) {
        oqlIndexManager.waitForIndexInit();
      }
      try {
        if (hasRemoteOrigin && !isTXHost && !isClientTXOriginator) {
          // If we are not a mirror then only apply the update to existing
          // entries
          //
          // If we are a mirror then then only apply the update to
          // existing entries when the operation is an update and we
          // are initialized.
          // Otherwise use the standard create/update logic
          if (!owner.isAllEvents() || (!putOp.isCreate() && isRegionReady)) {
            cbEventInPending = applyTxUpdateOnReplicateOrRedundantCopy(key, nv, didDestroy, txEvent,
                aCallbackArgument, pendingCallbacks, txEntryState, owner, putOp, newValue,
                hasRemoteOrigin, cbEvent, invokeCallbacks, cbEventInPending, opCompleted);
            return;
          }
        }
        RegionEntry newRe = getEntryFactory().createEntry(owner, key, Token.REMOVED_PHASE1);
        synchronized (newRe) {
          try {
            RegionEntry oldRe = putEntryIfAbsent(key, newRe);
            while (!opCompleted && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  owner.getCachePerfStats().incRetries();
                  getEntryMap().remove(key, oldRe);
                  oldRe = putEntryIfAbsent(key, newRe);
                } else {
                  opCompleted = true;
                  if (!oldRe.isRemoved()) {
                    putOp = putOp.getCorrespondingUpdateOp();
                  }
                  // Net writers are not called for received transaction data
                  final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                  final boolean oldIsRemoved = oldRe.isDestroyedOrRemoved();
                  if (cbEvent != null) {
                    cbEvent.setRegionEntry(oldRe);
                    cbEvent.setOldValue(oldRe.getValueInVM(owner)); // OFFHEAP eei
                  }
                  boolean clearOccured = false;
                  // Set RegionEntry updateInProgress
                  if (owner.getIndexMaintenanceSynchronous()) {
                    oldRe.setUpdateInProgress(true);
                  }
                  try {
                    txRemoveOldIndexEntry(putOp, oldRe);
                    if (didDestroy) {
                      oldRe.txDidDestroy(owner.cacheTimeMillis());
                    }
                    if (txEvent != null) {
                      txEvent.addPut(putOp, owner, oldRe, oldRe.getKey(), newValue,
                          aCallbackArgument);
                    }
                    oldRe.setValueResultOfSearch(putOp.isNetSearch());
                    try {
                      processAndGenerateTXVersionTag(owner, cbEvent, oldRe, txEntryState);
                      boolean wasTombstone = oldRe.isTombstone();
                      {
                        oldRe.setValue(owner, oldRe.prepareValueForCache(owner, newValue, cbEvent,
                            !putOp.isCreate()));
                        if (wasTombstone) {
                          owner.unscheduleTombstone(oldRe);
                        }
                      }
                      if (putOp.isCreate()) {
                        owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(oldRe));
                      } else if (putOp.isUpdate()) {
                        // Rahul : fix for 41694. Negative bucket size can also be
                        // an issue with normal GFE Delta and will have to be fixed
                        // in a similar manner and may be this fix the the one for
                        // other delta can be combined.
                        {
                          owner.updateSizeOnPut(key, oldSize,
                              owner.calculateRegionEntryValueSize(oldRe));
                        }
                      }
                    } catch (RegionClearedException rce) {
                      clearOccured = true;
                    }
                    {
                      long lastMod = owner.cacheTimeMillis();
                      EntryLogger.logTXPut(_getOwnerObject(), key, nv);
                      oldRe.updateStatsForPut(lastMod, lastMod);
                      owner.txApplyPutPart2(oldRe, oldRe.getKey(), lastMod, false, didDestroy,
                          clearOccured);
                    }
                  } finally {
                    if (oldRe != null && owner.getIndexMaintenanceSynchronous()) {
                      oldRe.setUpdateInProgress(false);
                    }
                  }
                  if (invokeCallbacks) {
                    if (!oldIsRemoved) {
                      cbEvent.makeUpdate();
                    }
                    switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                    if (pendingCallbacks == null) {
                      owner.invokeTXCallbacks(
                          cbEvent.op.isCreate() ? EnumListenerEvent.AFTER_CREATE
                              : EnumListenerEvent.AFTER_UPDATE,
                          cbEvent, true/* callDispatchListenerEvent */);
                    } else {
                      pendingCallbacks.add(cbEvent);
                      cbEventInPending = true;
                    }
                  }
                  if (!clearOccured) {
                    lruEntryUpdate(oldRe);
                  }
                }
              }
            }
            if (!opCompleted) {
              putOp = putOp.getCorrespondingCreateOp();
              if (cbEvent != null) {
                cbEvent.setRegionEntry(newRe);
                cbEvent.setOldValue(null);
              }
              boolean clearOccured = false;
              // Set RegionEntry updateInProgress
              if (owner.getIndexMaintenanceSynchronous()) {
                newRe.setUpdateInProgress(true);
              }
              try {
                txRemoveOldIndexEntry(putOp, newRe);
                // creating a new entry
                if (didDestroy) {
                  newRe.txDidDestroy(owner.cacheTimeMillis());
                }
                if (txEvent != null) {
                  txEvent.addPut(putOp, owner, newRe, newRe.getKey(), newValue, aCallbackArgument);
                }
                newRe.setValueResultOfSearch(putOp.isNetSearch());
                try {

                  processAndGenerateTXVersionTag(owner, cbEvent, newRe, txEntryState);
                  {
                    newRe.setValue(owner,
                        newRe.prepareValueForCache(owner, newValue, cbEvent, !putOp.isCreate()));
                  }
                  owner.updateSizeOnCreate(newRe.getKey(),
                      owner.calculateRegionEntryValueSize(newRe));
                } catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                {
                  long lastMod = owner.cacheTimeMillis();
                  EntryLogger.logTXPut(_getOwnerObject(), key, nv);
                  newRe.updateStatsForPut(lastMod, lastMod);
                  owner.txApplyPutPart2(newRe, newRe.getKey(), lastMod, true, didDestroy,
                      clearOccured);
                }
              } finally {
                if (newRe != null && owner.getIndexMaintenanceSynchronous()) {
                  newRe.setUpdateInProgress(false);
                }
              }
              opCompleted = true;
              if (invokeCallbacks) {
                cbEvent.makeCreate();
                cbEvent.setOldValue(null);
                switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                if (pendingCallbacks == null) {
                  owner.invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, cbEvent,
                      true/* callDispatchListenerEvent */);
                } else {
                  pendingCallbacks.add(cbEvent);
                  cbEventInPending = true;
                }
              }
              if (!clearOccured) {
                lruEntryCreate(newRe);
                incEntryCount(1);
              }
            }
          } finally {
            if (!opCompleted) {
              removeEntry(key, newRe, false);
            }
          }
        }
        if (owner.getConcurrencyChecksEnabled() && txEntryState != null && cbEvent != null) {
          txEntryState.setVersionTag(cbEvent.getVersionTag());
        }
      } catch (DiskAccessException dae) {
        owner.handleDiskAccessException(dae);
        throw dae;
      } finally {
        if (oqlIndexManager != null) {
          oqlIndexManager.countDownIndexUpdaters();
        }
      }
    } finally {
      if (!cbEventInPending)
        cbEvent.release();
    }
  }

  private boolean applyTxUpdateOnReplicateOrRedundantCopy(Object key, Object nv, boolean didDestroy,
      TXRmtEvent txEvent, Object aCallbackArgument, List<EntryEventImpl> pendingCallbacks,
      TXEntryState txEntryState, LocalRegion owner, Operation putOp, Object newValue,
      boolean hasRemoteOrigin, EntryEventImpl cbEvent, boolean invokeCallbacks,
      boolean cbEventInPending, boolean opCompleted) {
    // At this point we should only apply the update if the entry exists
    RegionEntry re = getEntry(key); // Fix for bug 32347.
    if (re != null) {
      synchronized (re) {
        if (!re.isRemoved()) {
          opCompleted = true;
          putOp = putOp.getCorrespondingUpdateOp();
          // Net writers are not called for received transaction data
          final int oldSize = owner.calculateRegionEntryValueSize(re);
          if (cbEvent != null) {
            cbEvent.setRegionEntry(re);
            cbEvent.setOldValue(re.getValueInVM(owner)); // OFFHEAP eei
          }

          boolean clearOccured = false;
          // Set RegionEntry updateInProgress
          if (owner.getIndexMaintenanceSynchronous()) {
            re.setUpdateInProgress(true);
          }
          try {
            txRemoveOldIndexEntry(putOp, re);
            if (didDestroy) {
              re.txDidDestroy(owner.cacheTimeMillis());
            }
            if (txEvent != null) {
              txEvent.addPut(putOp, owner, re, re.getKey(), newValue, aCallbackArgument);
            }
            re.setValueResultOfSearch(putOp.isNetSearch());
            try {
              processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
              {
                re.setValue(owner,
                    re.prepareValueForCache(owner, newValue, cbEvent, !putOp.isCreate()));
              }
              if (putOp.isCreate()) {
                owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(re));
              } else if (putOp.isUpdate()) {
                // Rahul : fix for 41694. Negative bucket size can also be
                // an issue with normal GFE Delta and will have to be fixed
                // in a similar manner and may be this fix the the one for
                // other delta can be combined.
                {
                  owner.updateSizeOnPut(key, oldSize, owner.calculateRegionEntryValueSize(re));
                }
              }
            } catch (RegionClearedException rce) {
              clearOccured = true;
            }
            {
              long lastMod = owner.cacheTimeMillis();
              EntryLogger.logTXPut(_getOwnerObject(), key, nv);
              re.updateStatsForPut(lastMod, lastMod);
              owner.txApplyPutPart2(re, re.getKey(), lastMod, false, didDestroy, clearOccured);
            }
          } finally {
            if (re != null && owner.getIndexMaintenanceSynchronous()) {
              re.setUpdateInProgress(false);
            }
          }
          if (invokeCallbacks) {
            cbEventInPending = prepareUpdateCallbacks(pendingCallbacks, owner, hasRemoteOrigin,
                cbEvent, cbEventInPending);
          }
          if (!clearOccured) {
            lruEntryUpdate(re);
          }
        }
      }
      if (didDestroy && !opCompleted) {
        owner.txApplyInvalidatePart2(re, re.getKey(), true, false /* clear */);
      }
    }
    if (invokeCallbacks && !opCompleted) {
      cbEvent.makeUpdate();
      owner.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, cbEvent, false);
    }
    if (owner.getConcurrencyChecksEnabled() && txEntryState != null && cbEvent != null) {
      txEntryState.setVersionTag(cbEvent.getVersionTag());
    }
    return cbEventInPending;
  }

  private boolean prepareUpdateCallbacks(List<EntryEventImpl> pendingCallbacks, LocalRegion owner,
      boolean hasRemoteOrigin, EntryEventImpl cbEvent, boolean cbEventInPending) {
    cbEvent.makeUpdate();
    switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
    if (pendingCallbacks == null) {
      owner.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, cbEvent, hasRemoteOrigin);
    } else {
      pendingCallbacks.add(cbEvent);
      cbEventInPending = true;
    }
    return cbEventInPending;
  }

  private void txHandleWANEvent(final LocalRegion owner, EntryEventImpl cbEvent,
      TXEntryState txEntryState) {
    ((BucketRegion) owner).handleWANEvent(cbEvent);
    if (txEntryState != null) {
      txEntryState.setTailKey(cbEvent.getTailKey());
    }
  }

  /**
   * called from txApply* methods to process and generate versionTags.
   */
  private void processAndGenerateTXVersionTag(final LocalRegion owner, EntryEventImpl cbEvent,
      RegionEntry re, TXEntryState txEntryState) {
    if (shouldPerformConcurrencyChecks(owner, cbEvent)) {
      try {
        if (txEntryState != null && txEntryState.getRemoteVersionTag() != null) {
          // to generate a version based on a remote VersionTag, we will
          // have to put the remote versionTag in the regionEntry
          VersionTag remoteTag = txEntryState.getRemoteVersionTag();
          if (re instanceof VersionStamp) {
            VersionStamp stamp = (VersionStamp) re;
            stamp.setVersions(remoteTag);
          }
        }
        processVersionTag(re, cbEvent);
      } catch (ConcurrentCacheModificationException ignore) {
        // ignore this execption, however invoke callbacks for this operation
      }

      // For distributed transactions, stuff the next region version generated
      // in phase-1 commit into the cbEvent so that ARE.generateVersionTag can later
      // just apply it and not regenerate it in phase-2 commit
      if (cbEvent != null && txEntryState != null && txEntryState.getDistTxEntryStates() != null) {
        cbEvent.setNextRegionVersion(txEntryState.getDistTxEntryStates().getRegionVersion());
      }

      // cbEvent.setNextRegionVersion(txEntryState.getNextRegionVersion());
      owner.generateAndSetVersionTag(cbEvent, re);
    }
  }

  /**
   * Checks for concurrency checks enabled on Region and that cbEvent is not null.
   */
  private boolean shouldPerformConcurrencyChecks(LocalRegion owner, EntryEventImpl cbEvent) {
    return owner.getConcurrencyChecksEnabled() && cbEvent != null;
  }

  /**
   * Switch the event's region from BucketRegion to owning PR and set originRemote to the given
   * value
   */
  static EntryEventImpl switchEventOwnerAndOriginRemote(EntryEventImpl event,
      boolean originRemote) {
    assert event != null;
    if (event.getRegion().isUsedForPartitionedRegionBucket()) {
      LocalRegion pr = event.getRegion().getPartitionedRegion();
      event.setRegion(pr);
    }
    event.setOriginRemote(originRemote);
    return event;
  }

  /**
   * Removing the existing indexed value requires the current value in the cache, that is the one
   * prior to applying the operation.
   *
   * @param entry the RegionEntry that contains the value prior to applying the op
   */
  private void txRemoveOldIndexEntry(Operation op, RegionEntry entry) {
    if ((op.isUpdate() && !entry.isInvalid()) || op.isInvalidate() || op.isDestroy()) {
      IndexManager idxManager = _getOwner().getIndexManager();
      if (idxManager != null) {
        try {
          idxManager.updateIndexes(entry, IndexManager.REMOVE_ENTRY,
              op.isUpdate() ? IndexProtocol.BEFORE_UPDATE_OP : IndexProtocol.OTHER_OP);
        } catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        }
      }
    }
  }

  public void dumpMap() {
    logger.info("dump of concurrent map of size {} for region {}", getEntryMap().size(),
        this._getOwner());
    for (Iterator it = getEntryMap().values().iterator(); it.hasNext();) {
      logger.info("dumpMap:" + it.next().toString());
    }
  }

  static boolean shouldCreateCBEvent(final LocalRegion owner, final boolean isInitialized) {
    LocalRegion lr = owner;
    boolean isPartitioned = lr.isUsedForPartitionedRegionBucket();

    if (isPartitioned) {
      /*
       * if(!((BucketRegion)lr).getBucketAdvisor().isPrimary()) {
       * if(!BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION) { return false; } }
       */
      lr = owner.getPartitionedRegion();
    }
    return (isPartitioned || isInitialized) && (lr.shouldDispatchListenerEvent()
        || lr.shouldNotifyBridgeClients() || lr.getConcurrencyChecksEnabled());
  }

  EntryEventImpl createCallBackEvent(final LocalRegion re, Operation op, Object key,
      Object newValue, TransactionId txId, TXRmtEvent txEvent, EventID eventId,
      Object aCallbackArgument, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    return createCBEvent(re, op, key, newValue, txId, txEvent, eventId, aCallbackArgument,
        filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
  }

  /** create a callback event for applying a transactional change to the local cache */
  @Retained
  public static EntryEventImpl createCBEvent(final InternalRegion internalRegion, Operation op,
      Object key, Object newValue, TransactionId txId, TXRmtEvent txEvent, EventID eventId,
      Object aCallbackArgument, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    DistributedMember originator = null;
    // txId should not be null even on localOrigin
    Assert.assertTrue(txId != null);
    originator = ((TXId) txId).getMemberId();

    InternalRegion eventRegion = internalRegion;
    if (eventRegion.isUsedForPartitionedRegionBucket()) {
      eventRegion = internalRegion.getPartitionedRegion();
    }

    @Retained
    EntryEventImpl retVal = EntryEventImpl.create(internalRegion, op, key, newValue,
        aCallbackArgument, txEntryState == null, originator);
    boolean returnedRetVal = false;
    try {


      if (bridgeContext != null) {
        retVal.setContext(bridgeContext);
      }

      if (eventRegion.generateEventID()) {
        retVal.setEventId(eventId);
      }

      if (versionTag != null) {
        retVal.setVersionTag(versionTag);
      }

      retVal.setTailKey(tailKey);

      FilterInfo localRouting = null;
      boolean computeFilterInfo = false;
      if (filterRoutingInfo == null) {
        computeFilterInfo = true;
      } else {
        localRouting = filterRoutingInfo.getLocalFilterInfo();
        if (localRouting != null) {
          // routing was computed in this VM but may need to perform local interest processing
          computeFilterInfo = !filterRoutingInfo.hasLocalInterestBeenComputed();
        } else {
          // routing was computed elsewhere and is in the "remote" routing table
          localRouting = filterRoutingInfo.getFilterInfo(internalRegion.getMyId());
        }
        if (localRouting != null) {
          if (!computeFilterInfo) {
            retVal.setLocalFilterInfo(localRouting);
          }
        } else {
          computeFilterInfo = true;
        }
      }
      if (logger.isTraceEnabled()) {
        logger.trace("createCBEvent filterRouting={} computeFilterInfo={} local routing={}",
            filterRoutingInfo, computeFilterInfo, localRouting);
      }

      if (internalRegion.isUsedForPartitionedRegionBucket()) {
        BucketRegion bucket = (BucketRegion) internalRegion;
        if (BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION
            || bucket.getBucketAdvisor().isPrimary()) {
          retVal.setInvokePRCallbacks(true);
        } else {
          retVal.setInvokePRCallbacks(false);
        }

        if (computeFilterInfo) {
          if (bucket.getBucketAdvisor().isPrimary()) {
            if (logger.isTraceEnabled()) {
              logger.trace("createCBEvent computing routing for primary bucket");
            }
            FilterProfile fp =
                ((BucketRegion) internalRegion).getPartitionedRegion().getFilterProfile();
            if (fp != null) {
              FilterRoutingInfo fri = fp.getFilterRoutingInfoPart2(filterRoutingInfo, retVal);
              if (fri != null) {
                retVal.setLocalFilterInfo(fri.getLocalFilterInfo());
              }
            }
          }
        }
      } else if (computeFilterInfo) { // not a bucket
        if (logger.isTraceEnabled()) {
          logger.trace("createCBEvent computing routing for non-bucket");
        }
        FilterProfile fp = internalRegion.getFilterProfile();
        if (fp != null) {
          retVal.setLocalFilterInfo(fp.getLocalFilterRouting(retVal));
        }
      }
      retVal.setTransactionId(txId);
      returnedRetVal = true;
      return retVal;
    } finally {
      if (!returnedRetVal) {
        retVal.release();
      }
    }
  }

  @Override
  public void writeSyncIfPresent(Object key, Runnable runner) {
    RegionEntry re = getEntry(key);
    if (re != null) {
      final boolean disabled = disableLruUpdateCallback();
      try {
        synchronized (re) {
          if (!re.isRemoved()) {
            runner.run();
          }
        }
      } finally {
        if (disabled) {
          enableLruUpdateCallback();
        }
        try {
          lruUpdateCallback();
        } catch (DiskAccessException dae) {
          this._getOwner().handleDiskAccessException(dae);
          throw dae;
        }
      }
    }
  }

  @Override
  public void removeIfDestroyed(Object key) {
    LocalRegion owner = _getOwner();
    // boolean makeTombstones = owner.concurrencyChecksEnabled;
    DiskRegion dr = owner.getDiskRegion();
    RegionEntry re = getEntry(key);
    if (re != null) {
      if (re.isDestroyed()) {
        synchronized (re) {
          if (re.isDestroyed()) {
            // [bruce] destroyed entries aren't in the LRU clock, so they can't be retained here
            // if (makeTombstones) {
            // re.makeTombstone(owner, re.getVersionStamp().asVersionTag());
            // } else {
            re.removePhase2();
            removeEntry(key, re, true);
          }
        }
      }
    }
    // }
  }

  /** get version-generation permission from the region's version vector */
  @Override
  public void lockForCacheModification(InternalRegion owner, EntryEventImpl event) {
    boolean lockedByBulkOp = event.isBulkOpInProgress() && owner.getDataPolicy().withReplication();

    if (armLockTestHook != null) {
      armLockTestHook.beforeLock(owner, event);
    }

    if (!event.isOriginRemote() && !lockedByBulkOp && !owner.hasServerProxy()) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.lockForCacheModification();
      }
    }

    if (armLockTestHook != null) {
      armLockTestHook.afterLock(owner, event);
    }
  }

  /** release version-generation permission from the region's version vector */
  @Override
  public void releaseCacheModificationLock(InternalRegion owner, EntryEventImpl event) {
    boolean lockedByBulkOp = event.isBulkOpInProgress() && owner.getDataPolicy().withReplication();

    if (armLockTestHook != null)
      armLockTestHook.beforeRelease(owner, event);

    if (!event.isOriginRemote() && !lockedByBulkOp && !owner.hasServerProxy()) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.releaseCacheModificationLock();
      }
    }

    if (armLockTestHook != null)
      armLockTestHook.afterRelease(owner, event);

  }

  @Override
  public void lockRegionForAtomicTX(InternalRegion r) {
    if (armLockTestHook != null)
      armLockTestHook.beforeLock(r, null);

    RegionVersionVector vector = r.getVersionVector();
    if (vector != null) {
      vector.lockForCacheModification();
    }

    if (armLockTestHook != null)
      armLockTestHook.afterLock(r, null);
  }

  @Override
  public void unlockRegionForAtomicTX(InternalRegion r) {
    if (armLockTestHook != null)
      armLockTestHook.beforeRelease(r, null);

    RegionVersionVector vector = r.getVersionVector();
    if (vector != null) {
      vector.releaseCacheModificationLock();
    }

    if (armLockTestHook != null)
      armLockTestHook.afterRelease(r, null);
  }

  /**
   * for testing race conditions between threads trying to apply ops to the same entry
   *
   * @param entry the entry to attempt to add to the system
   */
  protected RegionEntry putEntryIfAbsentForTest(RegionEntry entry) {
    return (RegionEntry) putEntryIfAbsent(entry.getKey(), entry);
  }

  @Override
  public boolean isTombstoneNotNeeded(RegionEntry re, int destroyedVersion) {
    // no need for synchronization - stale values are okay here
    // TODO this looks like a problem for regionEntry pooling
    if (getEntry(re.getKey()) != re) {
      // region entry was either removed (null)
      // or changed to a different region entry.
      // In either case the old tombstone is no longer needed.
      return true;
    }
    if (!re.isTombstone()) {
      // if the region entry no longer contains a tombstone
      // then the old tombstone is no longer needed
      return true;
    }
    VersionStamp<?> vs = re.getVersionStamp();
    if (vs == null) {
      // if we have no VersionStamp why were we even added as a tombstone?
      // We used to see an NPE here. See bug 52092.
      logger.error(
          "Unexpected RegionEntry scheduled as tombstone: re.getClass {} destroyedVersion {}",
          re.getClass(), destroyedVersion);
      return true;
    }
    if (vs.getEntryVersion() != destroyedVersion) {
      // the version changed so old tombstone no longer needed
      return true;
    }
    // region entry still has the same tombstone so we need to keep it.
    return false;
  }

  /** removes a tombstone that has expired locally */
  @Override
  public boolean removeTombstone(RegionEntry re, VersionHolder version, boolean isEviction,
      boolean isScheduledTombstone) {
    boolean result = false;
    int destroyedVersion = version.getEntryVersion();

    synchronized (this._getOwner().getSizeGuard()) { // do this sync first; see bug 51985
      synchronized (re) {
        int entryVersion = re.getVersionStamp().getEntryVersion();
        if (!re.isTombstone() || entryVersion > destroyedVersion) {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT_VERBOSE)) {
            logger.trace(LogMarker.TOMBSTONE_COUNT_VERBOSE,
                "tombstone for {} was resurrected with v{}; destroyed version was v{}; count is {}; entryMap size is {}",
                re.getKey(), re.getVersionStamp().getEntryVersion(), destroyedVersion,
                this._getOwner().getTombstoneCount(), size());
          }
        } else {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT_VERBOSE)) {
            if (entryVersion == destroyedVersion) {
              // logging this can put tremendous pressure on the log writer in tests
              // that "wait for silence"
              logger.trace(LogMarker.TOMBSTONE_COUNT_VERBOSE,
                  "removing tombstone for {} with v{} rv{}; count is {}", re.getKey(),
                  destroyedVersion, version.getRegionVersion(),
                  (this._getOwner().getTombstoneCount() - 1));
            } else {
              logger.trace(LogMarker.TOMBSTONE_COUNT_VERBOSE,
                  "removing entry (v{}) that is older than an expiring tombstone (v{} rv{}) for {}",
                  entryVersion, destroyedVersion, version.getRegionVersion(), re.getKey());
            }
          }
          try {
            re.setValue(_getOwner(), Token.REMOVED_PHASE2);
            if (removeTombstone(re)) {
              _getOwner().cancelExpiryTask(re);
              result = true;
              incEntryCount(-1);
              // Bug 51118: When the method is called by tombstoneGC thread, current 're' is an
              // expired tombstone. Then we detected an destroyed (due to overwritingOldTombstone()
              // returns true earlier) tombstone with bigger entry version, it's safe to delete
              // current tombstone 're' and adjust the tombstone count.
              // lruEntryDestroy(re); // tombstones are invisible to LRU
              if (isScheduledTombstone) {
                _getOwner().incTombstoneCount(-1);
              }
              RegionVersionVector vector = _getOwner().getVersionVector();
              if (vector != null) {
                vector.recordGCVersion(version.getMemberID(), version.getRegionVersion());
              }
            }
          } catch (RegionClearedException e) {
            // if the region has been cleared we don't need to remove the tombstone
          } catch (RegionDestroyedException e) {
            // if the region has been destroyed, the tombstone is already
            // gone. Catch an exception to avoid an error from the GC thread.
          }
        }
      }
    }
    return result;
  }

  private boolean removeTombstone(RegionEntry re) {
    return getEntryMap().remove(re.getKey(), re);
  }

  // method used for debugging tombstone count issues
  public boolean verifyTombstoneCount(AtomicInteger numTombstones) {
    int deadEntries = 0;
    try {
      for (Iterator it = getEntryMap().values().iterator(); it.hasNext();) {
        RegionEntry re = (RegionEntry) it.next();
        if (re.isTombstone()) {
          deadEntries++;
        }
      }
      if (deadEntries != numTombstones.get()) {
        if (logger.isDebugEnabled()) {
          logger.debug("tombstone count ({}) does not match actual number of tombstones ({})",
              numTombstones, deadEntries, new Exception());
        }
        return false;
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("tombstone count verified");
        }
      }
    } catch (Exception e) {
      // ignore
    }
    return true;
  }

  @Override
  public long getEvictions() {
    return 0;
  }

  @Override
  public void incRecentlyUsed() {
    // nothing by default
  }

  @Override
  public EvictionController getEvictionController() {
    return null;
  }

  @Override
  public int getEntryOverhead() {
    return (int) ReflectionSingleObjectSizer.sizeof(getEntryFactory().getEntryClass());
  }

  @Override
  public boolean beginChangeValueForm(EvictableEntry le,
      CachedDeserializable vmCachedDeserializable, Object v) {
    return false;
  }

  @Override
  public void finishChangeValueForm() {}

  @Override
  public int centralizedLruUpdateCallback() {
    return 0;
  }

  @Override
  public void updateEvictionCounter() {}

  public interface ARMLockTestHook {
    void beforeBulkLock(InternalRegion region);

    void afterBulkLock(InternalRegion region);

    void beforeBulkRelease(InternalRegion region);

    void afterBulkRelease(InternalRegion region);

    void beforeLock(InternalRegion region, CacheEvent event);

    void afterLock(InternalRegion region, CacheEvent event);

    void beforeRelease(InternalRegion region, CacheEvent event);

    void afterRelease(InternalRegion region, CacheEvent event);

    void beforeStateFlushWait();
  }

  private ARMLockTestHook armLockTestHook;

  @Override
  public ARMLockTestHook getARMLockTestHook() {
    return armLockTestHook;
  }

  public void setARMLockTestHook(ARMLockTestHook theHook) {
    armLockTestHook = theHook;
  }
}
