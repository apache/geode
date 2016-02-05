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
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.cache.region.entry.RegionEntryFactoryBuilder;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.query.IndexMaintenanceException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.internal.IndexUpdater;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.delta.Delta;
import com.gemstone.gemfire.internal.cache.ha.HAContainerWrapper;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.lru.LRUEntry;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionHolder;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.concurrent.MapCallbackAdapter;
import com.gemstone.gemfire.internal.concurrent.MapResult;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.offheap.Chunk;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.offheap.ReferenceCountHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.internal.sequencelog.EntryLogger;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializationException;
import com.gemstone.gemfire.pdx.internal.ConvertableToBytes;

/**
 * Abstract implementation of {@link RegionMap}that has all the common
 * behavior.
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */

//Asif: In case of sqlFabric System, we are creating a different set of RegionEntry 
// which are derived from the concrete  GFE RegionEntry classes.
// In future if any new concrete  RegionEntry class is defined, the new  SqlFabric
// RegionEntry Classes need to be created. There is a junit test in sqlfabric
// which checks for RegionEntry classes of GFE and validates the same with its 
// own classes.

public abstract class AbstractRegionMap implements RegionMap {

  private static final Logger logger = LogService.getLogger();
  
  /** The underlying map for this region. */
  protected CustomEntryConcurrentHashMap<Object, Object> map;
  /** An internal Listener for index maintenance for SQLFabric. */
  private final IndexUpdater indexUpdater;

  /**
   * This test hook is used to force the conditions for defect 48182.
   * This hook is used by Bug48182JUnitTest.
   */
  static Runnable testHookRunnableFor48182 =  null;
  
  private RegionEntryFactory entryFactory;
  private Attributes attr;
  private transient Object owner; // the region that owns this map
  
  protected AbstractRegionMap(InternalRegionArguments internalRegionArgs) {
    if (internalRegionArgs != null) {
      this.indexUpdater = internalRegionArgs.getIndexUpdater();
    }
    else {
      this.indexUpdater = null;
    }
  }

  public final IndexUpdater getIndexUpdater() {
    return this.indexUpdater;
  }

  protected void initialize(Object owner,
                            Attributes attr,
                            InternalRegionArguments internalRegionArgs,
                            boolean isLRU) {
    _setAttributes(attr);
    setOwner(owner);
    _setMap(createConcurrentMap(attr.initialCapacity, attr.loadFactor,
        attr.concurrencyLevel, false,
        new AbstractRegionEntry.HashRegionEntryCreator()));

    final GemFireCacheImpl cache;
    boolean isDisk;
    boolean withVersioning = false;
    boolean offHeap = false;
    if (owner instanceof LocalRegion) {
      LocalRegion region = (LocalRegion)owner;
      isDisk = region.getDiskRegion() != null;
      cache = region.getGemFireCache();
      withVersioning = region.getConcurrencyChecksEnabled();
      offHeap = region.getOffHeap();
    }
    else if (owner instanceof PlaceHolderDiskRegion) {
      offHeap = ((PlaceHolderDiskRegion) owner).getOffHeap();
      isDisk = true;
      withVersioning = ((PlaceHolderDiskRegion)owner).getFlags().contains(
          DiskRegionFlag.IS_WITH_VERSIONING);
      cache = GemFireCacheImpl.getInstance();
    }
    else {
      throw new IllegalStateException(
          "expected LocalRegion or PlaceHolderDiskRegion");
    }

    if (cache != null && cache.isSqlfSystem()) {
      String provider = GemFireCacheImpl.SQLF_ENTRY_FACTORY_PROVIDER;
      try {
        Class<?> factoryProvider = ClassPathLoader.getLatest().forName(provider);
        Method method = factoryProvider.getDeclaredMethod(
            "getRegionEntryFactory", new Class[] { Boolean.TYPE, Boolean.TYPE,
                Boolean.TYPE, Object.class, InternalRegionArguments.class });
        RegionEntryFactory ref = (RegionEntryFactory)method.invoke(null,
            new Object[] { Boolean.valueOf(attr.statisticsEnabled),
                Boolean.valueOf(isLRU), Boolean.valueOf(isDisk), owner,
                internalRegionArgs });

        // TODO need to have the SQLF entry factory support version stamp storage
        setEntryFactory(ref);

      }
      catch (Exception e) {
        throw new CacheRuntimeException(
            "Exception in obtaining RegionEntry Factory" + " provider class ",
            e) {
        };
      }
    }
    else {
      setEntryFactory(new RegionEntryFactoryBuilder().getRegionEntryFactoryOrNull(attr.statisticsEnabled,isLRU,isDisk,withVersioning,offHeap));
    }
  }

  protected CustomEntryConcurrentHashMap<Object, Object> createConcurrentMap(
      int initialCapacity, float loadFactor, int concurrencyLevel,
      boolean isIdentityMap,
      CustomEntryConcurrentHashMap.HashEntryCreator<Object, Object> entryCreator) {
    if (entryCreator != null) {
      return new CustomEntryConcurrentHashMap<Object, Object>(initialCapacity, loadFactor,
          concurrencyLevel, isIdentityMap, entryCreator);
    }
    else {
      return new CustomEntryConcurrentHashMap<Object, Object>(initialCapacity,
          loadFactor, concurrencyLevel, isIdentityMap);
    }
  }

  public void changeOwner(LocalRegion r) {
    if (r == _getOwnerObject()) {
      return;
    }
    setOwner(r);
  }

  @Override
  public final void setEntryFactory(RegionEntryFactory f) {
    this.entryFactory = f;
  }

  public final RegionEntryFactory getEntryFactory() {
    return this.entryFactory;
  }

  protected final void _setAttributes(Attributes a) {
    this.attr = a;
  }

  public final Attributes getAttributes() {
    return this.attr;
  }
  
  protected final LocalRegion _getOwner() {
    return (LocalRegion)this.owner;
  }

  protected boolean _isOwnerALocalRegion() {
    return this.owner instanceof LocalRegion;
  }

  protected final Object _getOwnerObject() {
    return this.owner;
  }

  public final void setOwner(Object r) {
    this.owner = r;
  }
  
  protected final CustomEntryConcurrentHashMap<Object, Object> _getMap() {
    return this.map;
  }

  protected final void _setMap(CustomEntryConcurrentHashMap<Object, Object> m) {
    this.map = m;
  }

  public int size()
  {
    return _getMap().size();
  }

  // this is currently used by stats and eviction
  @Override
  public int sizeInVM() {
    return _getMap().size();
  }

  public boolean isEmpty()
  {
    return _getMap().isEmpty();
  }

  public Set keySet()
  {
    return _getMap().keySet();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Collection<RegionEntry> regionEntries() {
    return (Collection)_getMap().values();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Collection<RegionEntry> regionEntriesInVM() {
    return (Collection)_getMap().values();
  }

  public final boolean containsKey(Object key) {
    RegionEntry re = getEntry(key);
    if (re == null) {
      return false;
    }
    if (re.isRemoved()) {
      return false;
    }
    return true;
  }

  public RegionEntry getEntry(Object key) {
    RegionEntry re = (RegionEntry)_getMap().get(key);
    if (re != null && re.isMarkedForEviction()) {
      // entry has been faulted in from HDFS
      return null;
    }
    return re;
  }

  protected RegionEntry getEntry(EntryEventImpl event) {
    return getEntry(event.getKey());
  }


  @Override
  public final RegionEntry getEntryInVM(Object key) {
    return (RegionEntry)_getMap().get(key);
  }


  public final RegionEntry putEntryIfAbsent(Object key, RegionEntry re) {
    RegionEntry value = (RegionEntry)_getMap().putIfAbsent(key, re);
    if (value == null && (re instanceof OffHeapRegionEntry) 
        && _isOwnerALocalRegion() && _getOwner().isThisRegionBeingClosedOrDestroyed()) {
      // prevent orphan during concurrent destroy (#48068)
      if (_getMap().remove(key, re)) {
        ((OffHeapRegionEntry)re).release();
      }
      _getOwner().checkReadiness(); // throw RegionDestroyedException
    }
    return value;
  }

  @Override
  public final RegionEntry getOperationalEntryInVM(Object key) {
    RegionEntry re = (RegionEntry)_getMap().get(key);
    if (re != null && re.isMarkedForEviction()) {
      // entry has been faulted in from HDFS
      return null;
    }
    return re;
  }
 

  public final void removeEntry(Object key, RegionEntry re, boolean updateStat) {
    if (re.isTombstone() && _getMap().get(key) == re && !re.isMarkedForEviction()){
      logger.fatal(LocalizedMessage.create(LocalizedStrings.AbstractRegionMap_ATTEMPT_TO_REMOVE_TOMBSTONE), new Exception("stack trace"));
      return; // can't remove tombstones except from the tombstone sweeper
    }
    if (_getMap().remove(key, re)) {
      re.removePhase2();
      if (updateStat) {
        incEntryCount(-1);
      }
    }
  }

  public final void removeEntry(Object key, RegionEntry re, boolean updateStat,
      EntryEventImpl event, final LocalRegion owner,
      final IndexUpdater indexUpdater) {
    boolean success = false;
    if (re.isTombstone()&& _getMap().get(key) == re && !re.isMarkedForEviction()) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.AbstractRegionMap_ATTEMPT_TO_REMOVE_TOMBSTONE), new Exception("stack trace"));
      return; // can't remove tombstones except from the tombstone sweeper
    }
    try {
      if (indexUpdater != null) {
        indexUpdater.onEvent(owner, event, re);
      }

      //This is messy, but custom eviction calls removeEntry
      //rather than re.destroy I think to avoid firing callbacks, etc.
      //However, the value still needs to be set to removePhase1
      //in order to remove the entry from disk.
      if(event.isCustomEviction() && !re.isRemoved()) {
        try {
          re.removePhase1(owner, false);
        } catch (RegionClearedException e) {
          //that's ok, we were just trying to do evict incoming eviction
        }
      }
      
      if (_getMap().remove(key, re)) {
        re.removePhase2();
        success = true;
        if (updateStat) {
          incEntryCount(-1);
        }
      }
    } finally {
      if (indexUpdater != null) {
        indexUpdater.postEvent(owner, event, re, success);
      }
    }
  }

  protected final void incEntryCount(int delta) {
    LocalRegion lr = _getOwner();
    if (lr != null) {
      CachePerfStats stats = lr.getCachePerfStats();
      if (stats != null) {
        stats.incEntryCount(delta);
      }
    }
  }
  
  final void incClearCount(LocalRegion lr) {
    if (lr != null && !(lr instanceof HARegion)) {
      CachePerfStats stats = lr.getCachePerfStats();
      if (stats != null) {
        stats.incClearCount();
      }
    }
  }

  private void _mapClear() {
    _getMap().clear();
  }
  
  public void close() {
    /*
    for (SuspectEntryList l: this.suspectEntries.values()) {
      for (EntryEventImpl e: l) {
        e.release();
      }
    }
    */
    clear(null);
  }
  
  /**
   * Clear the region and, if an RVV is given, return a collection of the
   * version sources in all remaining tags
   */
  public Set<VersionSource> clear(RegionVersionVector rvv)
  {
    Set<VersionSource> result = new HashSet<VersionSource>();
    
    if(!_isOwnerALocalRegion()) {
      //Fix for #41333. Just clear the the map
      //if we failed during initialization.
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
    Object lockObj = lr.getConcurrencyChecksEnabled()? lr.getSizeGuard() : new Object();
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
          synchronized(re) {
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
                logger.trace("region clear op is removing {} {}", re.getKey(), re.getVersionStamp());
              }

              boolean tombstone = re.isTombstone();
              // note: it.remove() did not reliably remove the entry so we use remove(K,V) here
              if (_getMap().remove(re.getKey(), re)) {
                if (OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap()) {
                  GatewaySenderEventImpl.release(re._getValue()); // OFFHEAP _getValue ok
                }
                //If this is an overflow only region, we need to free the entry on
                //disk at this point.
                try {
                  re.removePhase1(lr, true);
                } catch (RegionClearedException e) {
                  //do nothing, it's already cleared.
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
          logger.debug("Size after clearing = {}", _getMap().size());
        }
        if (isTraceEnabled && _getMap().size() < 20) {
          _getOwner().dumpBackingMap();
        }
      }
    }
    return result;
  }
  
  public void lruUpdateCallback()
  {
    // By default do nothing; LRU maps needs to override this method
  }
  public void lruUpdateCallback(boolean b)
  {
    // By default do nothing; LRU maps needs to override this method
  }
  public void lruUpdateCallback(int i)
  {
    // By default do nothing; LRU maps needs to override this method
  }

  public boolean disableLruUpdateCallback()
  {
    // By default do nothing; LRU maps needs to override this method
    return false;
  }

  public void enableLruUpdateCallback()
  {
    // By default do nothing; LRU maps needs to override this method
  }

  public void resetThreadLocals()
  {
    // By default do nothing; LRU maps needs to override this method
  }

  /**
   * Tell an LRU that a new entry has been created
   */
  protected void lruEntryCreate(RegionEntry e)
  {
    // do nothing by default
  }

  /**
   * Tell an LRU that an existing entry has been destroyed
   */
  protected void lruEntryDestroy(RegionEntry e)
  {
    // do nothing by default
  }

  /**
   * Tell an LRU that an existing entry has been modified
   */
  protected void lruEntryUpdate(RegionEntry e)
  {
    // do nothing by default
  }

  @Override
  public void decTxRefCount(RegionEntry e)
  {
    LocalRegion lr = null;
    if (_isOwnerALocalRegion()) {
      lr = _getOwner();
    }
    e.decRefCount(null, lr);
  }

  public boolean lruLimitExceeded() {
    return false;
  }

  public void lruCloseStats() {
    // do nothing by default
  }
  
  public void lruEntryFaultIn(LRUEntry entry) {
    // do nothing by default
  }
  
  /**
   * Process an incoming version tag for concurrent operation detection.
   * This must be done before modifying the region entry.
   * @param re the entry that is to be modified
   * @param event the modification to the entry
   * @throws InvalidDeltaException if the event contains a delta that cannot be applied
   * @throws ConcurrentCacheModificationException if the event is in conflict
   *    with a previously applied change
   */
  private void processVersionTag(RegionEntry re, EntryEventImpl event) {
    if (re.getVersionStamp() != null) {
      re.getVersionStamp().processVersionTag(event);
      
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
  
  private void processVersionTagForGII(RegionEntry re, LocalRegion owner, VersionTag entryVersion, boolean isTombstone, InternalDistributedMember sender, boolean checkConflicts) {
    
    re.getVersionStamp().processVersionTag(_getOwner(), entryVersion, isTombstone, false, owner.getMyId(), sender, checkConflicts);
  }

  public void copyRecoveredEntries(RegionMap rm) {
    //We need to sort the tombstones before scheduling them,
    //so that they will be in the correct order.
    OrderedTombstoneMap<RegionEntry> tombstones = new OrderedTombstoneMap<RegionEntry>();
    if (rm != null) {
      CustomEntryConcurrentHashMap<Object, Object> other = ((AbstractRegionMap)rm)._getMap();
      Iterator<Map.Entry<Object, Object>> it = other
          .entrySetWithReusableEntries().iterator();
      while (it.hasNext()) {
        Map.Entry<Object, Object> me = it.next();
        it.remove(); // This removes the RegionEntry from "rm" but it does not decrement its refcount to an offheap value.
        RegionEntry oldRe = (RegionEntry)me.getValue();
        Object key = me.getKey();
        
        @Retained @Released Object value = oldRe._getValueRetain((RegionEntryContext) ((AbstractRegionMap) rm)._getOwnerObject(), true);

        try {
          if (value == Token.NOT_AVAILABLE) {
            // fix for bug 43993
            value = null;
          }
          if (value == Token.TOMBSTONE && !_getOwner().getConcurrencyChecksEnabled()) {
            continue;
          }
          RegionEntry newRe = getEntryFactory().createEntry((RegionEntryContext) _getOwnerObject(), key, value);
          copyRecoveredEntry(oldRe, newRe);
          // newRe is now in this._getMap().
          if (newRe.isTombstone()) {
            VersionTag tag = newRe.getVersionStamp().asVersionTag();
            tombstones.put(tag, newRe);
          }
          _getOwner().updateSizeOnCreate(key, _getOwner().calculateRegionEntryValueSize(newRe));
          incEntryCount(1);
          lruEntryUpdate(newRe);
        } finally {
          if (OffHeapHelper.release(value)) {
            ((OffHeapRegionEntry)oldRe).release();
          }
        }
        lruUpdateCallback();
      }
    } else {
      incEntryCount(size());
      for (Iterator<RegionEntry> iter = regionEntries().iterator(); iter.hasNext(); ) {
        RegionEntry re = iter.next();
        if (re.isTombstone()) {
          if (re.getVersionStamp() == null) { // bug #50992 - recovery from versioned to non-versioned
            incEntryCount(-1);
            iter.remove();
            continue;
          } else {
            tombstones.put(re.getVersionStamp().asVersionTag(), re);
          }
        }
        _getOwner().updateSizeOnCreate(re.getKey(), _getOwner().calculateRegionEntryValueSize(re));
      }
      // Since lru was not being done during recovery call it now.
      lruUpdateCallback();
    }
    
    //Schedule all of the tombstones, now that we have sorted them
    Map.Entry<VersionTag, RegionEntry> entry;
    while((entry = tombstones.take()) != null) {
      // refresh the tombstone so it doesn't time out too soon
      _getOwner().scheduleTombstone(entry.getValue(), entry.getKey());
    }
    
  }
  
  protected void copyRecoveredEntry(RegionEntry oldRe, RegionEntry newRe) {
    if(newRe.getVersionStamp() != null) {
      newRe.getVersionStamp().setMemberID(oldRe.getVersionStamp().getMemberID());
      newRe.getVersionStamp().setVersions(oldRe.getVersionStamp().asVersionTag());
    }
    
    if (newRe instanceof AbstractOplogDiskRegionEntry) {
      ((AbstractOplogDiskRegionEntry)newRe).setDiskId(oldRe);
      _getOwner().getDiskRegion().replaceIncompatibleEntry((DiskEntry) oldRe, (DiskEntry) newRe);
    }
    _getMap().put(newRe.getKey(), newRe);
  }

  @Retained     // Region entry may contain an off-heap value
  public final RegionEntry initRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    boolean needsCallback = false;
    @Retained RegionEntry newRe = getEntryFactory().createEntry((RegionEntryContext) _getOwnerObject(), key, value);
    synchronized (newRe) {
      if (value.getVersionTag()!=null && newRe.getVersionStamp()!=null) {
        newRe.getVersionStamp().setVersions(value.getVersionTag());
      }
      RegionEntry oldRe = putEntryIfAbsent(key, newRe);
      while (oldRe != null) {
        synchronized (oldRe) {
          if (oldRe.isRemoved() && !oldRe.isTombstone()) {
            oldRe = putEntryIfAbsent(key, newRe);
            if (oldRe != null) {
              if (_isOwnerALocalRegion()) {
                _getOwner().getCachePerfStats().incRetries();
              }
            }
          } 
          /*
           * Entry already exists which should be impossible.
           * Free the current entry (if off-heap) and
           * throw an exception.
           */
          else {
            if (newRe instanceof OffHeapRegionEntry) {
              ((OffHeapRegionEntry) newRe).release();
            }

            throw new IllegalStateException("Could not recover entry for key " + key + ".  The entry already exists!");
          }
        } // synchronized
      }
      if (_isOwnerALocalRegion()) {
        _getOwner().updateSizeOnCreate(key, _getOwner().calculateRegionEntryValueSize(newRe));
        if (newRe.isTombstone()) {
          // refresh the tombstone so it doesn't time out too soon
          _getOwner().scheduleTombstone(newRe, newRe.getVersionStamp().asVersionTag());
        }
        
        incEntryCount(1); // we are creating an entry that was recovered from disk including tombstone
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
  
  public final RegionEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    boolean needsCallback = false;
    RegionEntry re = getEntry(key);
    if (re == null) {
      return null;
    }
    synchronized (re) {
      if (re.isRemoved() && !re.isTombstone()) {
        return null;
      }
      if (value.getVersionTag()!=null && re.getVersionStamp()!=null) {
        re.getVersionStamp().setVersions(value.getVersionTag());
      }
      try {
        if (_isOwnerALocalRegion()) {
          if (re.isTombstone()) {
            // when a tombstone is to be overwritten, unschedule it first
            _getOwner().unscheduleTombstone(re);
          }
          final int oldSize = _getOwner().calculateRegionEntryValueSize(re);
          re.setValue(_getOwner(), value); // OFFHEAP no need to call AbstractRegionMap.prepareValueForCache because setValue is overridden for disk and that code takes apart value (RecoveredEntry) and prepares its nested value for the cache
          if (re.isTombstone()) {
            _getOwner().scheduleTombstone(re, re.getVersionStamp().asVersionTag());
          }
          _getOwner().updateSizeOnPut(key, oldSize, _getOwner().calculateRegionEntryValueSize(re));
        } else {
          DiskEntry.Helper.updateRecoveredEntry((PlaceHolderDiskRegion)_getOwnerObject(),
              (DiskEntry)re, value, (RegionEntryContext) _getOwnerObject());
        }
      } catch (RegionClearedException rce) {
        throw new IllegalStateException("RegionClearedException should never happen in this context", rce);
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

  public final boolean initialImagePut(final Object key,
                                       final long lastModified,
                                       Object newValue,
                                       final boolean wasRecovered,
                                       boolean deferLRUCallback,
                                       VersionTag entryVersion, InternalDistributedMember sender, boolean isSynchronizing)
  {
    boolean result = false;
    boolean done = false;
    boolean cleared = false;
    final LocalRegion owner = _getOwner();
    
    if (newValue == Token.TOMBSTONE && !owner.getConcurrencyChecksEnabled()) {
      return false;
    }

    if (owner instanceof HARegion && newValue instanceof CachedDeserializable) {
      Object actualVal = ((CachedDeserializable)newValue)
        .getDeserializedValue(null, null);
      if (actualVal instanceof HAEventWrapper) {
        HAEventWrapper haEventWrapper = (HAEventWrapper)actualVal;
        // Key was removed at sender side so not putting it into the HARegion
        if (haEventWrapper.getClientUpdateMessage() == null) {
          return false;
        }
        // Getting the instance from singleton CCN..This assumes only one bridge
        // server in the VM
        HAContainerWrapper haContainer = (HAContainerWrapper)CacheClientNotifier
            .getInstance().getHaContainer();
        Map.Entry entry = null;
        HAEventWrapper original = null;
        synchronized (haContainer) {
          entry = (Map.Entry)haContainer.getEntry(haEventWrapper);
          if (entry != null) {
            original = (HAEventWrapper)entry.getKey();
            original.incAndGetReferenceCount();
          }
          else {
            haEventWrapper.incAndGetReferenceCount();
            haEventWrapper.setHAContainer(haContainer);
            haContainer.put(haEventWrapper, haEventWrapper
                .getClientUpdateMessage());
            haEventWrapper.setClientUpdateMessage(null);
            haEventWrapper.setIsRefFromHAContainer(true);
          }
        }
        if (entry != null) {
          HARegionQueue.addClientCQsAndInterestList(entry, haEventWrapper,
              haContainer, owner.getName());
          haEventWrapper.setClientUpdateMessage(null);
          newValue = CachedDeserializableFactory.create(original,
              ((CachedDeserializable)newValue).getSizeInBytes());
        }
      }
    }
    
    try {
      RegionEntry newRe = getEntryFactory().createEntry(owner, key,
          Token.REMOVED_PHASE1);
      EntryEventImpl event = null;
      
      @Retained @Released Object oldValue = null;
      
      try {
      RegionEntry oldRe = null;
      synchronized (newRe) {
        try {
          oldRe = putEntryIfAbsent(key, newRe);
          while (!done && oldRe != null) {
            synchronized (oldRe) {
              if (oldRe.isRemovedPhase2()) {
                oldRe = putEntryIfAbsent(key, newRe);
                if (oldRe != null) {
                  owner.getCachePerfStats().incRetries();
                }
              }
              else {
                boolean acceptedVersionTag = false;
                if (entryVersion != null && owner.concurrencyChecksEnabled) {
                  Assert.assertTrue(entryVersion.getMemberID() != null, "GII entry versions must have identifiers");
                  try {
                    boolean isTombstone = (newValue == Token.TOMBSTONE);
                    // don't reschedule the tombstone if it hasn't changed
                    boolean isSameTombstone = oldRe.isTombstone() && isTombstone
                              && oldRe.getVersionStamp().asVersionTag()
                                .equals(entryVersion);
                    if (isSameTombstone) {
                      return true;
                    }
                    processVersionTagForGII(oldRe, owner, entryVersion, isTombstone, sender, !wasRecovered || isSynchronizing);
                    acceptedVersionTag = true;
                  } catch (ConcurrentCacheModificationException e) {
                    return false;
                  }
                }
                final boolean oldIsTombstone = oldRe.isTombstone();
                final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                // Neeraj: The below if block is to handle the special
                // scenario witnessed in SqlFabric for now. (Though its
                // a general scenario). The scenario is that during GII
                // it is possible that updates start coming before the
                // base value reaches through GII. In that scenario the deltas
                // for that particular key is kept on being added to a list
                // of deltas. When the base value arrives through this path
                // of GII the oldValue will be that list of deltas. When the
                // base values arrives the deltas are applied one by one on that list.
                // The same scenario is applicable for GemFire also but the below 
                // code will be executed only in case of sqlfabric now. Probably
                // the code can be made more generic for both SQL Fabric and GemFire.
                if (indexUpdater != null) {
                  oldValue = oldRe.getValueInVM(owner); // OFFHEAP: ListOfDeltas
                  if (oldValue instanceof ListOfDeltas) {
                  // apply the deltas on this new value. update index
                  // Make a new event object
                  // make it an insert operation
                  LocalRegion rgn = owner;
                  if (owner instanceof BucketRegion) {
                    rgn = ((BucketRegion)owner).getPartitionedRegion();
                  }
                  event = EntryEventImpl.create(rgn, Operation.CREATE, key, null,
                      Boolean.TRUE /* indicate that GII is in progress */,
                      false, null);
                  try {
                  event.setOldValue(newValue);
                  if (logger.isDebugEnabled()) {
                    logger.debug("initialImagePut: received base value for list of deltas; event: {}", event);
                  }
                  ((ListOfDeltas)oldValue).apply(event);
                  Object preparedNewValue =oldRe.prepareValueForCache(owner,
                      event.getNewValueAsOffHeapDeserializedOrRaw(), true);
                  if(preparedNewValue instanceof Chunk) {
                    event.setNewValue(preparedNewValue);
                  }
                  oldRe.setValue(owner, preparedNewValue, event);
                  //event.setNewValue(event.getOldValue());
                  event.setOldValue(null);
                  try {
                    indexUpdater.onEvent(owner, event, oldRe);
                    lruEntryUpdate(oldRe);
                    owner.updateSizeOnPut(key, oldSize, owner.calculateRegionEntryValueSize(oldRe));
                    EntryLogger.logInitialImagePut(_getOwnerObject(), key, newValue);
                    result = true;
                    done = true;
                    break;
                  } finally {
                    // this must be done within the oldRe sync block
                    indexUpdater.postEvent(owner, event, oldRe, done);
                  }
                  } finally {
                    if (event != null) {
                      event.release();
                      event = null;
                    }
                  }
                  }
                }
                try {
                  if (indexUpdater != null) {
                    event = EntryEventImpl.create(owner, Operation.CREATE, key,
                        newValue,
                        Boolean.TRUE /* indicate that GII is in progress */,
                        false, null);
                    indexUpdater.onEvent(owner, event, oldRe);
                  }
                  result = oldRe.initialImagePut(owner, lastModified, newValue, wasRecovered, acceptedVersionTag);
                  if (result) {
                    if (oldIsTombstone) {
                      owner.unscheduleTombstone(oldRe);
                      if (newValue != Token.TOMBSTONE){
                        lruEntryCreate(oldRe);
                      } else {
                        lruEntryUpdate(oldRe);
                      }
                    }
                    if (newValue == Token.TOMBSTONE) {
                      if (owner.getServerProxy() == null &&
                          owner.getVersionVector().isTombstoneTooOld(entryVersion.getMemberID(), entryVersion.getRegionVersion())) {
                        // the received tombstone has already been reaped, so don't retain it
                        removeTombstone(oldRe, entryVersion, false, false);
                        return false;
                      } else {
                        owner.scheduleTombstone(oldRe, entryVersion);
                        lruEntryDestroy(oldRe);
                      }
                    } else {
                      int newSize = owner.calculateRegionEntryValueSize(oldRe);
                      if(!oldIsTombstone) {
                        owner.updateSizeOnPut(key, oldSize, newSize);
                      } else {
                        owner.updateSizeOnCreate(key, newSize);
                      }
                      EntryLogger.logInitialImagePut(_getOwnerObject(), key, newValue);
                    }
                  }
                  if (owner.getIndexManager() != null) {
                    owner.getIndexManager().updateIndexes(oldRe, oldRe.isRemoved() ? IndexManager.ADD_ENTRY : IndexManager.UPDATE_ENTRY, 
                        oldRe.isRemoved() ? IndexProtocol.OTHER_OP : IndexProtocol.AFTER_UPDATE_OP);
                  }
                  done = true;
                } finally {
                  if (indexUpdater != null) {
                    indexUpdater.postEvent(owner, event, oldRe, result);
                  }
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
            if (entryVersion != null && owner.concurrencyChecksEnabled) {
              Assert.assertTrue(entryVersion.getMemberID() != null, "GII entry versions must have identifiers");
              try {
                boolean isTombstone = (newValue == Token.TOMBSTONE);
                processVersionTagForGII(newRe, owner, entryVersion, isTombstone, sender, !wasRecovered || isSynchronizing);
                versionTagAccepted = true;
              } catch (ConcurrentCacheModificationException e) {
                return false;
              }
            }
            result = newRe.initialImageInit(owner, lastModified, newValue,
                true, wasRecovered, versionTagAccepted);
            try {
              if (result) {
                if (indexUpdater != null) {
                  event = EntryEventImpl.create(owner, Operation.CREATE, key,
                      newValue,
                      Boolean.TRUE /* indicate that GII is in progress */,
                      false, null);
                  indexUpdater.onEvent(owner, event, newRe);
                }
                if (newValue == Token.TOMBSTONE) {
                  owner.scheduleTombstone(newRe, entryVersion);
                } else {
                  owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(newRe));
                  EntryLogger.logInitialImagePut(_getOwnerObject(), key, newValue);
                  lruEntryCreate(newRe);
                }
                incEntryCount(1);
              }
              //Update local indexes
              if (owner.getIndexManager() != null) {
                owner.getIndexManager().updateIndexes(newRe, newRe.isRemoved() ? IndexManager.REMOVE_ENTRY : IndexManager.UPDATE_ENTRY, 
                    newRe.isRemoved() ? IndexProtocol.OTHER_OP : IndexProtocol.AFTER_UPDATE_OP);
              }
              done = true;
            } finally {
              if (result && indexUpdater != null) {
                indexUpdater.postEvent(owner, event, newRe, done);
              }
              if (event != null) {
                event.release();
                event = null;
              }
            }
          }
        }
        finally {
          if (done && result) {
            initialImagePutEntry(newRe);
          }
          if (!done) {
            removeEntry(key, newRe, false);
            if (owner.getIndexManager() != null) {
              owner.getIndexManager().updateIndexes(newRe, IndexManager.REMOVE_ENTRY, IndexProtocol.OTHER_OP);
            }
          }
       } 
      } // synchronized
      } finally {
        if (event != null) event.release();
        OffHeapHelper.release(oldValue);
      }
    } catch(RegionClearedException rce) {
      //Asif: do not issue any sort of callbacks
      done = false;
      cleared= true;
    }catch(QueryException qe) {
      done = false;
      cleared= true;
    }
    finally {
      if (done && !deferLRUCallback) {
        lruUpdateCallback();
      }
      else if (!cleared) {
        resetThreadLocals();
      }
    }
    return result;
  }

  protected void initialImagePutEntry(RegionEntry newRe) {
  }

  boolean confirmEvictionDestroy(RegionEntry re)
  {
    /* We arn't in an LRU context, and should never get here */
    Assert.assertTrue(false,
        "Not an LRU region, can not confirm LRU eviction operation");
    return true;
  }

  public final boolean destroy(EntryEventImpl event,
                               boolean inTokenMode,
                               boolean duringRI,
                               boolean cacheWrite,
                               boolean isEviction,
                               Object expectedOldValue,
                               boolean removeRecoveredEntry)
  throws CacheWriterException, EntryNotFoundException, TimeoutException {
    
    final LocalRegion owner = _getOwner();

    if (owner == null) {
      Assert.assertTrue(false, "The owner for RegionMap " + this    // "fix" for bug 32440
          + " is null for event " + event);
    }
    
    boolean retry = true;
    
    while (retry) {
      retry = false;

      boolean opCompleted = false;
      boolean doPart3 = false;

      // We need to acquire the region entry while holding the lock to avoid #45620.
      // However, we also want to release the lock before distribution to prevent
      // potential deadlocks.  The outer try/finally ensures that the lock will be
      // released without fail.  I'm avoiding indenting just to preserve the ability
      // to track diffs since the code is fairly complex.
      boolean doUnlock = true;
      lockForCacheModification(owner, event);
      try {


        RegionEntry re = getOrCreateRegionEntry(owner, event, Token.REMOVED_PHASE1, null, true, true); 
        RegionEntry tombstone = null;
        boolean haveTombstone = false;
        /*
         * Execute the test hook runnable inline (not threaded) if it is not null. 
         */
        if(null != testHookRunnableFor48182) {
          testHookRunnableFor48182.run();
        }    

        try {
          if (logger.isTraceEnabled(LogMarker.LRU_TOMBSTONE_COUNT) && !(owner instanceof HARegion)) {
            logger.trace(LogMarker.LRU_TOMBSTONE_COUNT,
                "ARM.destroy() inTokenMode={}; duringRI={}; riLocalDestroy={}; withRepl={}; fromServer={}; concurrencyEnabled={}; isOriginRemote={}; isEviction={}; operation={}; re={}",
                inTokenMode, duringRI, event.isFromRILocalDestroy(), owner.dataPolicy.withReplication(), event.isFromServer(),
                owner.concurrencyChecksEnabled, event.isOriginRemote(), isEviction, event.getOperation(), re);
          }
          if (event.isFromRILocalDestroy()) {
            // for RI local-destroy we don't want to keep tombstones.
            // In order to simplify things we just set this recovery
            // flag to true to force the entry to be removed
            removeRecoveredEntry = true;
          }
          // the logic in this method is already very involved, and adding tombstone
          // permutations to (re != null) greatly complicates it.  So, we check
          // for a tombstone here and, if found, pretend for a bit that the entry is null
          if (re != null && re.isTombstone() && !removeRecoveredEntry) {
            tombstone = re;
            haveTombstone = true;
            re = null;
          }
          IndexManager oqlIndexManager = owner.getIndexManager() ; 
          if (re == null) {
            // we need to create an entry if in token mode or if we've received
            // a destroy from a peer or WAN gateway and we need to retain version
            // information for concurrency checks
            boolean retainForConcurrency = (!haveTombstone
                && (owner.dataPolicy.withReplication() || event.isFromServer())
                && owner.concurrencyChecksEnabled
                && (event.isOriginRemote() /* destroy received from other must create tombstone */
                    || event.isFromWANAndVersioned() /* wan event must create a tombstone */
                    || event.isBridgeEvent())); /* event from client must create a tombstone so client has a version # */ 
            if (inTokenMode
                || retainForConcurrency) { 
              // removeRecoveredEntry should be false in this case
              RegionEntry newRe = getEntryFactory().createEntry(owner,
                  event.getKey(),
                  Token.REMOVED_PHASE1);
              // Fix for Bug #44431. We do NOT want to update the region and wait
              // later for index INIT as region.clear() can cause inconsistency if
              // happened in parallel as it also does index INIT.
              if (oqlIndexManager != null) {
                oqlIndexManager.waitForIndexInit();
              }
              try {
                synchronized (newRe) {
                  RegionEntry oldRe = putEntryIfAbsent(event.getKey(), newRe);
                  while (!opCompleted && oldRe != null) {
                    synchronized (oldRe) {
                      if (oldRe.isRemovedPhase2()) {
                        oldRe = putEntryIfAbsent(event.getKey(), newRe);
                        if (oldRe != null) {
                          owner.getCachePerfStats().incRetries();
                        }
                      } else {
                        event.setRegionEntry(oldRe);

                        // Last transaction related eviction check. This should
                        // prevent
                        // transaction conflict (caused by eviction) when the entry
                        // is being added to transaction state.
                        if (isEviction) {
                          if (!confirmEvictionDestroy(oldRe) || (owner.getEvictionCriteria() != null && !owner.getEvictionCriteria().doEvict(event))) {
                            opCompleted = false;
                            return opCompleted;
                          }
                        }
                        try {
                          //if concurrency checks are enabled, destroy will
                          //set the version tag
                          boolean destroyed = destroyEntry(oldRe, event, inTokenMode, cacheWrite, expectedOldValue, false, removeRecoveredEntry);
                          if (destroyed) {
                            if (retainForConcurrency) {
                              owner.basicDestroyBeforeRemoval(oldRe, event);
                            }
                            owner.basicDestroyPart2(oldRe, event, inTokenMode,
                                false /* conflict with clear */, duringRI, true);
                            lruEntryDestroy(oldRe);
                            doPart3 = true;
                          }
                        }
                        catch (RegionClearedException rce) {
                          // Ignore. The exception will ensure that we do not update
                          // the LRU List
                          owner.basicDestroyPart2(oldRe, event, inTokenMode,
                              true/* conflict with clear */, duringRI, true);
                          doPart3 = true;
                        } catch (ConcurrentCacheModificationException ccme) {
                          VersionTag tag = event.getVersionTag();
                          if (tag != null && tag.isTimeStampUpdated()) {
                            // Notify gateways of new time-stamp.
                            owner.notifyTimestampsToGateways(event);
                          }
                          throw ccme;
                        }
                        re = oldRe;
                        opCompleted = true;
                      }
                    } // synchronized oldRe
                  } // while
                  if (!opCompleted) {
                    // The following try has a finally that cleans up the newRe.
                    // This is only needed if newRe was added to the map which only
                    // happens if we didn't get completed with oldRe in the above while loop.
                    try {
                      re = newRe;
                      event.setRegionEntry(newRe);

                      try {
                        //if concurrency checks are enabled, destroy will
                        //set the version tag
                        if (isEviction) {
                          opCompleted = false;
                          return opCompleted; 
                        }
                        opCompleted = destroyEntry(newRe, event, inTokenMode, cacheWrite, expectedOldValue, true, removeRecoveredEntry);
                        if (opCompleted) {
                          // This is a new entry that was created because we are in
                          // token mode or are accepting a destroy operation by adding
                          // a tombstone.  There is no oldValue, so we don't need to
                          // call updateSizeOnRemove
                          //                    owner.recordEvent(event);
                          event.setIsRedestroyedEntry(true);  // native clients need to know if the entry didn't exist
                          if (retainForConcurrency) {
                            owner.basicDestroyBeforeRemoval(oldRe, event);
                          }
                          owner.basicDestroyPart2(newRe, event, inTokenMode,
                              false /* conflict with clear */, duringRI, true);
                          doPart3 = true;
                        }
                      }
                      catch (RegionClearedException rce) {
                        // Ignore. The exception will ensure that we do not update
                        // the LRU List
                        opCompleted = true;
                        EntryLogger.logDestroy(event);
                        owner.basicDestroyPart2(newRe, event, inTokenMode, true /* conflict with clear*/, duringRI, true);
                        doPart3 = true;
                      } catch (ConcurrentCacheModificationException ccme) {
                        VersionTag tag = event.getVersionTag();
                        if (tag != null && tag.isTimeStampUpdated()) {
                          // Notify gateways of new time-stamp.
                          owner.notifyTimestampsToGateways(event);
                        }
                        throw ccme;
                      }
                      // Note no need for LRU work since the entry is destroyed
                      // and will be removed when gii completes
                    } finally {
                      if (!opCompleted && !haveTombstone  /* to fix bug 51583 do this for all operations */ ) {
                        removeEntry(event.getKey(), newRe, false);
                      }
                      if (!opCompleted && isEviction) {
                        removeEntry(event.getKey(), newRe, false);
                      }
                    }
                  } // !opCompleted
                } // synchronized newRe
              } finally {
                if (oqlIndexManager != null) {
                  oqlIndexManager.countDownIndexUpdaters();
                }
              }
            } // inTokenMode or tombstone creation
            else {
              if (!isEviction || owner.concurrencyChecksEnabled) {                                 
                // The following ensures that there is not a concurrent operation
                // on the entry and leaves behind a tombstone if concurrencyChecksEnabled.
                // It fixes bug #32467 by propagating the destroy to the server even though
                // the entry isn't in the client
                RegionEntry newRe = haveTombstone? tombstone : getEntryFactory().createEntry(owner, event.getKey(),
                    Token.REMOVED_PHASE1);
                synchronized(newRe) {
                  if (haveTombstone && !tombstone.isTombstone()) {
                    // we have to check this again under synchronization since it may have changed
                    retry = true;
                    //retryEntry = tombstone; // leave this in place for debugging
                    continue;
                  }
                  re = (RegionEntry)_getMap().putIfAbsent(event.getKey(), newRe);
                  if (re != null && re != tombstone) {
                    // concurrent change - try again
                    retry = true;
                    //retryEntry = tombstone; // leave this in place for debugging
                    continue;
                  }
                  else if (!isEviction) {
                    boolean throwex = false;
                    EntryNotFoundException ex =  null;
                    try {
                      if (!cacheWrite) {
                        throwex = true;
                      } else {
                        try {
                          if (!removeRecoveredEntry) {
                            throwex = !owner.bridgeWriteBeforeDestroy(event, expectedOldValue);
                          }
                        } catch (EntryNotFoundException e) {
                          throwex = true;
                          ex = e; 
                        }
                      }
                      if (throwex) {
                        if (!event.isOriginRemote() && !event.getOperation().isLocal() &&
                            (event.isFromBridgeAndVersioned() ||  // if this is a replayed client event that already has a version
                                event.isFromWANAndVersioned())) { // or if this is a WAN event that has been applied in another system
                          // we must distribute these since they will update the version information in peers
                          if (logger.isDebugEnabled()) {
                            logger.debug("ARM.destroy is allowing wan/client destroy of {} to continue", event.getKey());
                          }
                          throwex = false;
                          event.setIsRedestroyedEntry(true);
                          // Distribution of this op happens on re and re might me null here before
                          // distributing this destroy op.
                          if (re == null) {
                            re = newRe;
                          }
                          doPart3 = true;
                        }
                      }
                      if (throwex) {                    
                        if (ex == null) {
                          // Fix for 48182, check cache state and/or region state before sending entry not found.
                          // this is from the server and any exceptions will propogate to the client
                          owner.checkEntryNotFound(event.getKey());
                        } else {
                          throw ex;
                        }
                      }
                    } finally {
                      // either remove the entry or leave a tombstone
                      try {
                        if (!event.isOriginRemote() && event.getVersionTag() != null && owner.concurrencyChecksEnabled) {
                          // this shouldn't fail since we just created the entry.
                          // it will either generate a tag or apply a server's version tag
                          processVersionTag(newRe, event);
                          if (doPart3) {
                            owner.generateAndSetVersionTag(event, newRe);
                          }
                          try {
                            owner.recordEvent(event);
                            newRe.makeTombstone(owner, event.getVersionTag());
                          } catch (RegionClearedException e) {
                            // that's okay - when writing a tombstone into a disk, the
                            // region has been cleared (including this tombstone)
                          }
                          opCompleted = true;
                          //                    lruEntryCreate(newRe);
                        } else if (!haveTombstone) {
                          try {
                            assert newRe != tombstone;
                            newRe.setValue(owner, Token.REMOVED_PHASE2);
                            removeEntry(event.getKey(), newRe, false);
                          } catch (RegionClearedException e) {
                            // that's okay - we just need to remove the new entry
                          }
                        } else if (event.getVersionTag() != null ) { // haveTombstone - update the tombstone version info
                          processVersionTag(tombstone, event);
                          if (doPart3) {
                            owner.generateAndSetVersionTag(event, newRe);
                          }
                          // This is not conflict, we need to persist the tombstone again with new version tag 
                          try {
                            tombstone.setValue(owner, Token.TOMBSTONE);
                          } catch (RegionClearedException e) {
                            // that's okay - when writing a tombstone into a disk, the
                            // region has been cleared (including this tombstone)
                          }
                          owner.recordEvent(event);
                          owner.rescheduleTombstone(tombstone, event.getVersionTag());
                          owner.basicDestroyPart2(tombstone, event, inTokenMode, true /* conflict with clear*/, duringRI, true);
                          opCompleted = true;
                        }
                      } catch (ConcurrentCacheModificationException ccme) {
                        VersionTag tag = event.getVersionTag();
                        if (tag != null && tag.isTimeStampUpdated()) {
                          // Notify gateways of new time-stamp.
                          owner.notifyTimestampsToGateways(event);
                        }
                        throw ccme;
                      }
                    }
                  }
                } // synchronized(newRe)
              }
            }
          } // no current entry
          else { // current entry exists
            if (oqlIndexManager != null) {
              oqlIndexManager.waitForIndexInit();
            }
            try {
              synchronized (re) {
                // if the entry is a tombstone and the event is from a peer or a client
                // then we allow the operation to be performed so that we can update the
                // version stamp.  Otherwise we would retain an old version stamp and may allow
                // an operation that is older than the destroy() to be applied to the cache
                // Bug 45170: If removeRecoveredEntry, we treat tombstone as regular entry to be deleted
                boolean createTombstoneForConflictChecks = (owner.concurrencyChecksEnabled
                    && (event.isOriginRemote() || event.getContext() != null || removeRecoveredEntry));
                if (!re.isRemoved() || createTombstoneForConflictChecks) {
                  if (re.isRemovedPhase2()) {
                    retry = true;
                    continue;
                  }
                  if (!event.isOriginRemote() && event.getOperation().isExpiration()) {
                    // If this expiration started locally then only do it if the RE is not being used by a tx.
                    if (re.isInUseByTransaction()) {
                      opCompleted = false;
                      return opCompleted;
                    }
                  }
                  event.setRegionEntry(re);

                  // See comment above about eviction checks
                  if (isEviction) {
                    assert expectedOldValue == null;
                    if (!confirmEvictionDestroy(re) || (owner.getEvictionCriteria() != null && !owner.getEvictionCriteria().doEvict(event))) {
                      opCompleted = false;
                      return opCompleted;
                    }
                  }

                  boolean removed = false;
                  try {
                    opCompleted = destroyEntry(re, event, inTokenMode, cacheWrite, expectedOldValue, false, removeRecoveredEntry);
                    if (opCompleted) {
                      // It is very, very important for Partitioned Regions to keep
                      // the entry in the map until after distribution occurs so that other
                      // threads performing a create on this entry wait until the destroy
                      // distribution is finished.
                      // keeping backup copies consistent. Fix for bug 35906.
                      // -- mthomas 07/02/2007 <-- how about that date, kinda cool eh?
                      owner.basicDestroyBeforeRemoval(re, event);

                      // do this before basicDestroyPart2 to fix bug 31786
                      if (!inTokenMode) {
                        if ( re.getVersionStamp() == null) {
                          re.removePhase2();
                          removeEntry(event.getKey(), re, true, event, owner,
                              indexUpdater);
                          removed = true;
                        }
                      }
                      if (inTokenMode && !duringRI) {
                        event.inhibitCacheListenerNotification(true);
                      }
                      doPart3 = true;
                      owner.basicDestroyPart2(re, event, inTokenMode, false /* conflict with clear*/, duringRI, true);
                      //                  if (!re.isTombstone() || isEviction) {
                      lruEntryDestroy(re);
                      //                  } else {
                      //                    lruEntryUpdate(re);
                      //                    lruUpdateCallback = true;
                      //                  }
                    } else {
                      if (!inTokenMode) {
                        EntryLogger.logDestroy(event);
                        owner.recordEvent(event);
                        if (re.getVersionStamp() == null) {
                          re.removePhase2();
                          removeEntry(event.getKey(), re, true, event, owner,
                              indexUpdater);
                          lruEntryDestroy(re);
                        } else {
                          if (re.isTombstone()) {
                            // the entry is already a tombstone, but we're destroying it
                            // again, so we need to reschedule the tombstone's expiration
                            if (event.isOriginRemote()) {
                              owner.rescheduleTombstone(re, re.getVersionStamp().asVersionTag());
                            }
                          }
                        }
                        lruEntryDestroy(re);
                        opCompleted = true;
                      }
                    }
                  }
                  catch (RegionClearedException rce) {
                    // Ignore. The exception will ensure that we do not update
                    // the LRU List
                    opCompleted = true;
                    owner.recordEvent(event);
                    if (inTokenMode && !duringRI) {
                      event.inhibitCacheListenerNotification(true);
                    }
                    owner.basicDestroyPart2(re, event, inTokenMode, true /*conflict with clear*/, duringRI, true);
                    doPart3 = true;
                  }
                  finally {
                    if (re.isRemoved() && !re.isTombstone()) {
                      if (!removed) {
                        removeEntry(event.getKey(), re, true, event, owner,
                            indexUpdater);
                      }
                    }
                  }
                } // !isRemoved
                else { // already removed
                  if (owner.isHDFSReadWriteRegion() && re.isRemovedPhase2()) {
                    // For HDFS region there may be a race with eviction
                    // so retry the operation. fixes bug 49150
                    retry = true;
                    continue;
                  }
                  if (re.isTombstone() && event.getVersionTag() != null) {
                    // if we're dealing with a tombstone and this is a remote event
                    // (e.g., from cache client update thread) we need to update
                    // the tombstone's version information
                    // TODO use destroyEntry() here
                    processVersionTag(re, event);
                    try {
                      re.makeTombstone(owner, event.getVersionTag());
                    } catch (RegionClearedException e) {
                      // that's okay - when writing a tombstone into a disk, the
                      // region has been cleared (including this tombstone)
                    }
                  }
                  if (expectedOldValue != null) {
                    // if re is removed then there is no old value, so return false
                    return false;
                  }

                  if (!inTokenMode && !isEviction) {
                    owner.checkEntryNotFound(event.getKey());
                  }
                }
              } // synchronized re
            }  catch (ConcurrentCacheModificationException ccme) {
              VersionTag tag = event.getVersionTag();
              if (tag != null && tag.isTimeStampUpdated()) {
                // Notify gateways of new time-stamp.
                owner.notifyTimestampsToGateways(event);
              }
              throw ccme;
            } finally {
              if (oqlIndexManager != null) {
                oqlIndexManager.countDownIndexUpdaters();
              }
            }
            // No need to call lruUpdateCallback since the only lru action
            // we may have taken was lruEntryDestroy. This fixes bug 31759.

          } // current entry exists
          if(opCompleted) {
            EntryLogger.logDestroy(event);
          }
          return opCompleted;
        }
        finally {
          releaseCacheModificationLock(owner, event);
          doUnlock = false;

          try {
            // If concurrency conflict is there and event contains gateway version tag then
            // do NOT distribute.
            if (event.isConcurrencyConflict() &&
                (event.getVersionTag() != null && event.getVersionTag().isGatewayTag())) {
              doPart3 = false;
            }
            // distribution and listener notification
            if (doPart3) {
              owner.basicDestroyPart3(re, event, inTokenMode, duringRI, true, expectedOldValue);
            }
          } finally {
            if (opCompleted) {
              if (re != null) {
                owner.cancelExpiryTask(re);
              } else if (tombstone != null) {
                owner.cancelExpiryTask(tombstone);
              }
            }
          }
        }

      } finally { // failsafe on the read lock...see comment above
        if (doUnlock) {
          releaseCacheModificationLock(owner, event);
        }
      }
    } // retry loop
    return false;
  }

  public final void txApplyDestroy(Object key, TransactionId txId,
      TXRmtEvent txEvent, boolean inTokenMode, boolean inRI, Operation op, 
      EventID eventId, Object aCallbackArgument,List<EntryEventImpl> pendingCallbacks,FilterRoutingInfo filterRoutingInfo,ClientProxyMembershipID bridgeContext,
      boolean isOriginRemote, TXEntryState txEntryState, VersionTag versionTag, long tailKey)
  {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    final LocalRegion owner = _getOwner();
    owner.checkBeforeEntrySync(txEvent);

    final boolean isRegionReady = !inTokenMode;
    final boolean hasRemoteOrigin = !((TXId)txId).getMemberId().equals(owner.getMyId());
    boolean cbEventInPending = false;
    lockForTXCacheModification(owner, versionTag);
    IndexManager oqlIndexManager = owner.getIndexManager() ; 
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
              EntryEventImpl sqlfEvent = null;
              @Retained @Released Object oldValue = re.getValueInVM(owner);
              try {
              final int oldSize = owner.calculateRegionEntryValueSize(re);
              // Create an entry event only if the calling context is
              // a receipt of a TXCommitMessage AND there are callbacks installed
              // for this region
              boolean invokeCallbacks = shouldCreateCBEvent(owner, false/*isInvalidate*/, isRegionReady || inRI);
              EntryEventImpl cbEvent = createCBEvent(owner, op,
                  key, null, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
              try {
              
              if (/* owner.isUsedForPartitionedRegionBucket() && */ 
                  indexUpdater != null) {
                 sqlfEvent = cbEvent;
              } else {
                if (owner.isUsedForPartitionedRegionBucket()) {
                  txHandleWANEvent(owner, cbEvent, txEntryState);
                }
                cbEvent.setRegionEntry(re);
              }
              cbEvent.setOldValue(oldValue);
              if (isDebugEnabled) {
                logger.debug("txApplyDestroy cbEvent={}", cbEvent);
              }
              
              txRemoveOldIndexEntry(Operation.DESTROY, re);
              if (txEvent != null) {
                txEvent.addDestroy(owner, re, re.getKey(),aCallbackArgument);
              }
              boolean clearOccured = false;
              try {
                processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
                if (inTokenMode) {
                  if (oldValue == Token.TOMBSTONE) {
                    owner.unscheduleTombstone(re);
                  }
                  re.setValue(owner, Token.DESTROYED);
                }
                else {
                  if (!re.isTombstone()) {
                    if (sqlfEvent != null) {
                      re.removePhase1(owner, false); // fix for bug 43063
                      re.removePhase2();
                      removeEntry(key, re, true, sqlfEvent, owner, indexUpdater);
                    } else {
                      if (shouldPerformConcurrencyChecks(owner, cbEvent) && cbEvent.getVersionTag() != null) {
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
              }
              catch (RegionClearedException rce) {
                clearOccured = true;
              }
              owner.txApplyDestroyPart2(re, re.getKey(), inTokenMode,
                  clearOccured /* Clear Conflciting with the operation */);
              if (invokeCallbacks) {
                switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                if(pendingCallbacks==null) {
                  owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY,
                      cbEvent, true/*callDispatchListenerEvent*/);
                } else {
                  pendingCallbacks.add(cbEvent);
                  cbEventInPending = true;
                }
              }
              if (!clearOccured) {
                lruEntryDestroy(re);
              }
              if (owner.concurrencyChecksEnabled && txEntryState != null && cbEvent!= null) {
                txEntryState.setVersionTag(cbEvent.getVersionTag());
              }
              } finally {
                if (!cbEventInPending) cbEvent.release();
              }
              } finally {
                OffHeapHelper.release(oldValue);
              }
            }
          }
        } finally {
          if (oqlIndexManager != null) {
            oqlIndexManager.countDownIndexUpdaters();
          }
        }
      } else if (inTokenMode || owner.concurrencyChecksEnabled) {
        // treating tokenMode and re == null as same, since we now want to
        // generate versions and Tombstones for destroys
        boolean dispatchListenerEvent = inTokenMode;
        boolean opCompleted = false;
        RegionEntry newRe = getEntryFactory().createEntry(owner, key,
            Token.DESTROYED);
        if ( oqlIndexManager != null) {
          oqlIndexManager.waitForIndexInit();
        }
        EntryEventImpl cbEvent = null;
        try {
          synchronized (newRe) {
            RegionEntry oldRe = putEntryIfAbsent(key, newRe);
            while (!opCompleted && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  oldRe = putEntryIfAbsent(key, newRe);
                  if (oldRe != null) {
                    owner.getCachePerfStats().incRetries();
                  }
                }
                else {
                  try {
                    boolean invokeCallbacks = shouldCreateCBEvent(owner, false, isRegionReady || inRI);
                    cbEvent = createCBEvent(owner, op,
                        key, null, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
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
                      if(pendingCallbacks==null) {
                        owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY,
                            cbEvent, dispatchListenerEvent);
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
                      if (!cbEventInPending) cbEvent.release();
                    }
                  }
                  catch (RegionClearedException rce) {
                    owner.txApplyDestroyPart2(oldRe, oldRe.getKey(), inTokenMode,
                        true /* Clear Conflicting with the operation */);
                  }
                  if (shouldPerformConcurrencyChecks(owner, cbEvent) && cbEvent.getVersionTag() != null) {
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
              boolean invokeCallbacks = shouldCreateCBEvent(owner, false, isRegionReady || inRI);
              cbEvent = createCBEvent(owner, op,
                  key, null, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
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
                if(pendingCallbacks==null) {
                  owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY,
                      cbEvent, dispatchListenerEvent);
                } else {
                  pendingCallbacks.add(cbEvent);
                  cbEventInPending = true;
                }
              }
              EntryLogger.logTXDestroy(_getOwnerObject(), key);
              owner.updateSizeOnCreate(newRe.getKey(), 0);
              if (shouldPerformConcurrencyChecks(owner, cbEvent) && cbEvent.getVersionTag() != null) {
                newRe.makeTombstone(owner, cbEvent.getVersionTag());
              } else if (!inTokenMode) {
                // only remove for NORMAL regions if they do not generate versions see 51781
                newRe.removePhase1(owner, false); // fix for bug 43063
                newRe.removePhase2();
                removeEntry(key, newRe, false);
              }
              owner
                  .txApplyDestroyPart2(newRe, newRe.getKey(), inTokenMode,
                      false /*clearConflict*/);
              // Note no need for LRU work since the entry is destroyed
              // and will be removed when gii completes
              } finally {
                if (!cbEventInPending) cbEvent.release();
              }
            }
            if (owner.concurrencyChecksEnabled && txEntryState != null && cbEvent != null) {
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
        EntryEventImpl cbEvent = createCBEvent(owner, op,
            key, null, txId, txEvent, eventId, aCallbackArgument, 
            filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
        try {
        if (owner.isUsedForPartitionedRegionBucket()) {
          txHandleWANEvent(owner, cbEvent, txEntryState);
        }
        switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
        if (pendingCallbacks == null) {
          owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY,cbEvent,false);
        } else {
          pendingCallbacks.add(cbEvent);
          cbEventInPending = true;
        }
        } finally {
          if (!cbEventInPending) cbEvent.release();
        }
      }
    } catch( DiskAccessException dae) {
      owner.handleDiskAccessException(dae);
      throw dae;
    }
    finally {
      releaseTXCacheModificationLock(owner, versionTag);
    }
  }

  /**
   * If true then invalidates that throw EntryNotFoundException
   * or that are already invalid will first call afterInvalidate on CacheListeners. 
   * The old value on the event passed to afterInvalidate will be null.
   */
  public static boolean FORCE_INVALIDATE_EVENT = Boolean.getBoolean("gemfire.FORCE_INVALIDATE_EVENT");

  /**
   * If the FORCE_INVALIDATE_EVENT flag is true
   * then invoke callbacks on the given event.
   */
  void forceInvalidateEvent(EntryEventImpl event) {
    if (FORCE_INVALIDATE_EVENT) {
      event.invokeCallbacks(_getOwner(), false, false);
    }
  }
  
  public final boolean invalidate(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry, boolean forceCallbacks)
      throws EntryNotFoundException
  {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this
          + " is null for event " + event);

    }
    boolean didInvalidate = false;
    RegionEntry invalidatedRe = null;
    boolean clearOccured = false;

    DiskRegion dr = owner.getDiskRegion();
    // Fix for Bug #44431. We do NOT want to update the region and wait
    // later for index INIT as region.clear() can cause inconsistency if
    // happened in parallel as it also does index INIT.
    IndexManager oqlIndexManager = owner.getIndexManager() ; 
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    lockForCacheModification(owner, event);
    try {
      if (forceNewEntry || forceCallbacks) {
        boolean opCompleted = false;
        RegionEntry newRe = getEntryFactory().createEntry(owner, event.getKey(),
            Token.REMOVED_PHASE1);
          synchronized (newRe) {
            try {
              RegionEntry oldRe = putEntryIfAbsent(event.getKey(), newRe);
              
              while (!opCompleted && oldRe != null) {
                synchronized (oldRe) {
                  // if the RE is in phase 2 of removal, it will really be removed
                  // from the map.  Otherwise, we can use it here and the thread
                  // that is destroying the RE will see the invalidation and not
                  // proceed to phase 2 of removal.
                  if (oldRe.isRemovedPhase2()) {
                    oldRe = putEntryIfAbsent(event.getKey(), newRe);
                    if (oldRe != null) {
                      owner.getCachePerfStats().incRetries();
                    }
                  } else {
                    opCompleted = true;
                    event.setRegionEntry(oldRe);
                    if (oldRe.isDestroyed()) {
                      if (isDebugEnabled) {
                        logger.debug("mapInvalidate: Found DESTROYED token, not invalidated; key={}", event.getKey());
                      }
                    } else if (oldRe.isInvalid()) {
                    
                      // was already invalid, do not invoke listeners or increment stat
                      if (isDebugEnabled) {
                        logger.debug("mapInvalidate: Entry already invalid: '{}'", event.getKey());
                      }
                      processVersionTag(oldRe, event);
                      try {
                        oldRe.setValue(owner, oldRe.getValueInVM(owner)); // OFFHEAP noop setting an already invalid to invalid; No need to call prepareValueForCache since it is an invalid token.
                      } catch (RegionClearedException e) {
                        // that's okay - when writing an invalid into a disk, the
                        // region has been cleared (including this token)
                      }
                      forceInvalidateEvent(event);
                    } else {
                      owner.cacheWriteBeforeInvalidate(event, invokeCallbacks, forceNewEntry);
                      if (owner.concurrencyChecksEnabled && event.noVersionReceivedFromServer()) {
                        // server did not perform the invalidation, so don't leave an invalid
                        // entry here
                        return false;
                      }
                      final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                      //added for cq which needs old value. rdubey
                      FilterProfile fp = owner.getFilterProfile();
                      if (!oldRe.isRemoved() && 
                          (fp != null && fp.getCqCount() > 0)) {
                        
                        @Retained @Released Object oldValue = oldRe.getValueInVM(owner); // OFFHEAP EntryEventImpl oldValue
                        
                        // this will not fault in the value.
                        try {
                        if (oldValue == Token.NOT_AVAILABLE){
                          event.setOldValue(oldRe.getValueOnDiskOrBuffer(owner));
                        } else {
                          event.setOldValue(oldValue);
                        }
                        } finally {
                          OffHeapHelper.release(oldValue);
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
                            owner.updateSizeOnPut(event.getKey(), oldSize, event.getNewValueBucketSize());
                          } else {
                            owner.updateSizeOnCreate(event.getKey(), event.getNewValueBucketSize());
                            isCreate = true;
                          }
                        } else {
                          processVersionTag(oldRe, event);
                          event.putExistingEntry(owner, oldRe);
                          EntryLogger.logInvalidate(event);
                          owner.recordEvent(event);
                          owner.updateSizeOnPut(event.getKey(), oldSize, event.getNewValueBucketSize());
                        }
                      }
                      catch (RegionClearedException e) {
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
                  event.inhibitCacheListenerNotification(true);
                }
                event.setRegionEntry(newRe);
                owner.cacheWriteBeforeInvalidate(event, invokeCallbacks, forceNewEntry);
                if (!forceNewEntry && event.noVersionReceivedFromServer()) {
                  // server did not perform the invalidation, so don't leave an invalid
                  // entry here
                  return false;
                }
                try {
                  if (!owner.isInitialized() && owner.getDataPolicy().withReplication()) {
                    final int oldSize = owner.calculateRegionEntryValueSize(newRe);
                    invalidateEntry(event, newRe, oldSize);
                  }
                  else {
                    invalidateNewEntry(event, owner, newRe);
                  }
                }
                catch (RegionClearedException e) {
                  // TODO: deltaGII: do we even need RegionClearedException?
                  // generate versionTag for the event
                  owner.recordEvent(event);
                  clearOccured = true;
                }
                owner.basicInvalidatePart2(newRe, event, clearOccured /*conflict with clear*/, invokeCallbacks);
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
            if (!owner.isInitialized()) {
              // when GII message arrived or processed later than invalidate
              // message, the entry should be created as placeholder
              RegionEntry newRe = haveTombstone? tombstone : getEntryFactory().createEntry(owner, event.getKey(),
                  Token.INVALID);
              synchronized (newRe) {
                if (haveTombstone && !tombstone.isTombstone()) {
                  // state of the tombstone has changed so we need to retry
                  retry = true;
                  //retryEntry = tombstone; // leave this in place for debugging
                  continue;
                }
                re = putEntryIfAbsent(event.getKey(), newRe);
                if (re == tombstone) {
                  re = null; // pretend we don't have an entry
                }
              }
            } else if (owner.getServerProxy() != null) {
              Object sync = haveTombstone? tombstone : new Object();
              synchronized(sync) {
                if (haveTombstone && !tombstone.isTombstone()) { 
                  // bug 45295: state of the tombstone has changed so we need to retry
                  retry = true;
                  //retryEntry = tombstone; // leave this in place for debugging
                  continue;
                }
       
                // bug #43287 - send event to server even if it's not in the client (LRU may have evicted it)
                owner.cacheWriteBeforeInvalidate(event, true, false);
                if (owner.concurrencyChecksEnabled) {
                  if (event.getVersionTag() == null) {
                    // server did not perform the invalidation, so don't leave an invalid
                    // entry here
                    return false;
                  } else if (tombstone != null) {
                    processVersionTag(tombstone, event);
                    try {
                      if (!tombstone.isTombstone()) {
                        if (isDebugEnabled) {
                          logger.debug("tombstone is no longer a tombstone. {}:event={}", tombstone, event);
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
                    // update the tombstone's version to prevent an older CCU/putAll from overwriting it
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
                // If this expiration started locally then only do it if the RE is not being used by a tx.
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
                    owner.getVersionVector().recordVersion((InternalDistributedMember) event.getDistributedMember(), event.getVersionTag());
                  }
                  forceInvalidateEvent(event);
                }
                else { // previous value not invalid
                  event.setRegionEntry(re);
                  owner.cacheWriteBeforeInvalidate(event, invokeCallbacks, forceNewEntry);
                  if (owner.concurrencyChecksEnabled && event.noVersionReceivedFromServer()) {
                    // server did not perform the invalidation, so don't leave an invalid
                    // entry here
                    if (isDebugEnabled) {
                      logger.debug("returning early because server did not generate a version stamp for this event:{}", event);
                    }
                    return false;
                  }
             // in case of overflow to disk we need the old value for cqs.
                  if(owner.getFilterProfile().getCqCount() > 0){
                    //use to be getValue and can cause dead lock rdubey.
                    if (re.isValueNull()) {
                      event.setOldValue(re.getValueOnDiskOrBuffer(owner));
                    } else {
                      
                      @Retained @Released Object v = re.getValueInVM(owner);
                      
                      try {
                        event.setOldValue(v); // OFFHEAP escapes to EntryEventImpl oldValue
                      } finally {
                        OffHeapHelper.release(v);
                      }
                    }
                  }
                  final boolean oldWasTombstone = re.isTombstone();
                  final int oldSize = _getOwner().calculateRegionEntryValueSize(re);
                  try {
                    invalidateEntry(event, re, oldSize);
                  }
                  catch (RegionClearedException rce) {
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
                  owner.basicInvalidatePart2(re, event,
                      clearOccured /* conflict with clear */, invokeCallbacks);
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
            forceInvalidateEvent(event);
            owner.checkEntryNotFound(event.getKey());
          }
        } // while(retry)
      } // !forceNewEntry
    } catch( DiskAccessException dae) {
      invalidatedRe = null;
      didInvalidate = false;
      this._getOwner().handleDiskAccessException(dae);
      throw dae;
    } finally {
      releaseCacheModificationLock(owner, event);
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
      if (invalidatedRe != null) {
        owner.basicInvalidatePart3(invalidatedRe, event, invokeCallbacks);
      }
      if (didInvalidate && !clearOccured) {
        try {
          lruUpdateCallback();
        } catch( DiskAccessException dae) {
          this._getOwner().handleDiskAccessException(dae);
          throw dae;
        }
      }
      else if (!didInvalidate){
        resetThreadLocals();
      }
    }
    return didInvalidate;
  }

  protected void invalidateNewEntry(EntryEventImpl event,
      final LocalRegion owner, RegionEntry newRe) throws RegionClearedException {
    processVersionTag(newRe, event);
    event.putNewEntry(owner, newRe);
    owner.recordEvent(event);
    owner.updateSizeOnCreate(event.getKey(), event.getNewValueBucketSize());
  }

  protected void invalidateEntry(EntryEventImpl event, RegionEntry re,
      int oldSize) throws RegionClearedException {
    processVersionTag(re, event);
    event.putExistingEntry(_getOwner(), re);
    EntryLogger.logInvalidate(event);
    _getOwner().recordEvent(event);
    _getOwner().updateSizeOnPut(event.getKey(), oldSize, event.getNewValueBucketSize());
  }

  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.RegionMap#updateEntryVersion(com.gemstone.gemfire.internal.cache.EntryEventImpl)
   */
  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {

    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this
          + " is null for event " + event);

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
            if (re.isTombstone()
                || (!re.isRemoved() && !re.isDestroyed())) {
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
    }  catch( DiskAccessException dae) {
      this._getOwner().handleDiskAccessException(dae);
      throw dae;
    } finally {
      releaseCacheModificationLock(owner, event);
      if (dr != null) {
        dr.removeClearCountReference();
      }
    }
  }

  public final void txApplyInvalidate(Object key, Object newValue, boolean didDestroy,
      TransactionId txId, TXRmtEvent txEvent, boolean localOp, 
      EventID eventId, Object aCallbackArgument,List<EntryEventImpl> pendingCallbacks,FilterRoutingInfo filterRoutingInfo,ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag, long tailKey)
  {
//    boolean didInvalidate = false;
    final LocalRegion owner = _getOwner();
    owner.checkBeforeEntrySync(txEvent);
    
    EntryEventImpl cbEvent = null;
    boolean forceNewEntry = !owner.isInitialized() && owner.isAllEvents();

    final boolean hasRemoteOrigin = !((TXId)txId).getMemberId().equals(owner.getMyId());
    DiskRegion dr = owner.getDiskRegion();
    // Fix for Bug #44431. We do NOT want to update the region and wait
    // later for index INIT as region.clear() can cause inconsistency if
    // happened in parallel as it also does index INIT.
    IndexManager oqlIndexManager = owner.getIndexManager() ; 
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    lockForTXCacheModification(owner, versionTag);
    try {
      if (forceNewEntry) {
        boolean opCompleted = false;
        RegionEntry newRe = getEntryFactory().createEntry(owner, key,
            Token.REMOVED_PHASE1);
          synchronized (newRe) {
            try {
              RegionEntry oldRe = putEntryIfAbsent(key, newRe);
              while (!opCompleted && oldRe != null) {
                synchronized (oldRe) {
                  if (oldRe.isRemovedPhase2()) {
                    oldRe = putEntryIfAbsent(key, newRe);
                    if (oldRe != null) {
                      owner.getCachePerfStats().incRetries();
                    }
                  }
                  else {
                    opCompleted = true;
                    final boolean oldWasTombstone = oldRe.isTombstone();
                    final int oldSize = owner.calculateRegionEntryValueSize(oldRe);
                    Object oldValue = oldRe.getValueInVM(owner); // OFFHEAP eei
                    try {
                    // Create an entry event only if the calling context is
                    // a receipt of a TXCommitMessage AND there are callbacks
                    // installed
                    // for this region
                    boolean invokeCallbacks = shouldCreateCBEvent(owner, true, owner.isInitialized());
                    boolean cbEventInPending = false;
                    cbEvent = createCBEvent(owner, 
                        localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE,
                        key, newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
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
                      txEvent.addInvalidate(owner, oldRe, oldRe.getKey(),
                          newValue,aCallbackArgument);
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
                    }
                    catch (RegionClearedException rce) {
                      clearOccured = true;
                    }
                    owner.txApplyInvalidatePart2(oldRe, oldRe.getKey(),
                        didDestroy, true, clearOccured);
  //                  didInvalidate = true;
                    if (invokeCallbacks) {
                      switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                      if(pendingCallbacks==null) {
                        owner.invokeTXCallbacks(
                            EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                            true/*callDispatchListenerEvent*/);
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
                      if (!cbEventInPending) cbEvent.release();
                    }
                    } finally {
                      OffHeapHelper.release(oldValue);
                    }
                  }
                }
              }
              if (!opCompleted) {
                boolean invokeCallbacks = shouldCreateCBEvent( owner, true /* isInvalidate */, owner.isInitialized());
                boolean cbEventInPending = false;
                cbEvent = createCBEvent(owner, 
                    localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE,
                        key, newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
                try {
                cbEvent.setRegionEntry(newRe);
                txRemoveOldIndexEntry(Operation.INVALIDATE, newRe);
                newRe.setValueResultOfSearch(false);
                boolean clearOccured = false;
                try {
                  processAndGenerateTXVersionTag(owner, cbEvent, newRe, txEntryState);
                  newRe.setValue(owner, newRe.prepareValueForCache(owner, newValue, true));
                  EntryLogger.logTXInvalidate(_getOwnerObject(), key);
                  owner.updateSizeOnCreate(newRe.getKey(), 0);//we are putting in a new invalidated entry
                }
                catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                owner.txApplyInvalidatePart2(newRe, newRe.getKey(), didDestroy,
                    true, clearOccured);
                
                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if(pendingCallbacks==null) {
                    owner.invokeTXCallbacks(
                        EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                        true/*callDispatchListenerEvent*/);
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
                  if (!cbEventInPending) cbEvent.release();
                }
              }
            }
            finally {
              if (!opCompleted) {
                removeEntry(key, newRe, false);
              }
            }
          }
      }
      else { /* !forceNewEntry */
        RegionEntry re = getEntry(key);
        if (re != null) {
            synchronized (re) {
              {
                final int oldSize = owner.calculateRegionEntryValueSize(re);
                boolean wasTombstone = re.isTombstone();
                Object oldValue = re.getValueInVM(owner); // OFFHEAP eei
                // Create an entry event only if the calling context is
                // a receipt of a TXCommitMessage AND there are callbacks
                // installed
                // for this region
                boolean invokeCallbacks = shouldCreateCBEvent(owner, true, owner.isInitialized());
                boolean cbEventInPending = false;
                cbEvent = createCBEvent(owner, 
                    localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, 
                        key, newValue, txId, txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
                try {
                cbEvent.setRegionEntry(re);
                cbEvent.setOldValue(oldValue);
                txRemoveOldIndexEntry(Operation.INVALIDATE, re);
                if (didDestroy) {
                  re.txDidDestroy(owner.cacheTimeMillis());
                }
                if (txEvent != null) {
                  txEvent.addInvalidate(owner, re, re.getKey(), newValue,aCallbackArgument);
                }
                re.setValueResultOfSearch(false);
                processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
                boolean clearOccured = false;
                try {
                  re.setValue(owner, re.prepareValueForCache(owner, newValue, true));
                  EntryLogger.logTXInvalidate(_getOwnerObject(), key);
                  if (wasTombstone) {
                    owner.unscheduleTombstone(re);
                  }
                  owner.updateSizeOnPut(key, oldSize, 0);
                }
                catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                owner.txApplyInvalidatePart2(re, re.getKey(), didDestroy, true,
                    clearOccured);
  //              didInvalidate = true;
                if (invokeCallbacks) {
                  switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                  if(pendingCallbacks==null) {
                    owner.invokeTXCallbacks(
                        EnumListenerEvent.AFTER_INVALIDATE, cbEvent,
                        true/*callDispatchListenerEvent*/);
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
                  if (!cbEventInPending) cbEvent.release();
                }
              }
            }
        } else  { //re == null
          // Fix bug#43594
          // In cases where bucket region is re-created, it may so happen 
          // that the invalidate is already applied on the Initial image 
          // provider, thus causing region entry to be absent. 
          // Notify clients with client events.
          boolean cbEventInPending = false;
          cbEvent = createCBEvent(owner, 
              localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, 
                  key, newValue, txId, txEvent, eventId, aCallbackArgument, 
                  filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
          try {
          switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
          if (pendingCallbacks == null) {
            owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE,
                cbEvent, false);
          } else {
            pendingCallbacks.add(cbEvent);
            cbEventInPending = true;
          }
          } finally {
            if (!cbEventInPending) cbEvent.release();
          }
        }
      }
    }catch( DiskAccessException dae) {
      owner.handleDiskAccessException(dae);
      throw dae;
    }
    finally {
      releaseTXCacheModificationLock(owner, versionTag);
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
    }
  }

  /**
   * This code needs to be evaluated. It was added quickly to help PR persistence
   * not to consume as much memory.
   */
  public void evictValue(Object key) {
    final LocalRegion owner = _getOwner();
    RegionEntry re = getEntry(key);
    if (re != null) {
      synchronized (re) {
        if (!re.isValueNull()) {
          re.setValueToNull();
          owner.getDiskRegion().incNumEntriesInVM(-1L);
          owner.getDiskRegion().incNumOverflowOnDisk(1L);
          if(owner instanceof BucketRegion)
          {
            ((BucketRegion)owner).incNumEntriesInVM(-1L);
            ((BucketRegion)owner).incNumOverflowOnDisk(1L);
          }
        }
      }
    }
  }

  private RegionEntry getOrCreateRegionEntry(Object ownerRegion,
      EntryEventImpl event, Object value,
      MapCallbackAdapter<Object, Object, Object, Object> valueCreator,
      boolean onlyExisting, boolean returnTombstone) {
    Object key = event.getKey();
    RegionEntry retVal = null;
    if (event.isFetchFromHDFS()) {
      retVal = getEntry(event);
    } else {
      retVal = getEntryInVM(key);
    }
    if (onlyExisting) {
      if (!returnTombstone && (retVal != null && retVal.isTombstone())) {
        return null;
      }
      return retVal;
    }
    if (retVal != null) {
      return retVal;
    }
    if (valueCreator != null) {
      value = valueCreator.newValue(key, ownerRegion, value, null);
    }
    retVal = getEntryFactory().createEntry((RegionEntryContext) ownerRegion, key, value);
    RegionEntry oldRe = putEntryIfAbsent(key, retVal);
    if (oldRe != null) {
      if (retVal instanceof OffHeapRegionEntry) {
        ((OffHeapRegionEntry) retVal).release();
      }
      return oldRe;
    }
    return retVal;
  }

  protected static final MapCallbackAdapter<Object, Object, Object, Object>
      listOfDeltasCreator = new MapCallbackAdapter<Object, Object,
          Object, Object>() {
    @Override
    public Object newValue(Object key, Object context, Object createParams,
        final MapResult result) {
      return new ListOfDeltas(4);
    }
  };
  
  /**
   * Neeraj: The below if block is to handle the special
   * scenario witnessed in Sqlfabric for now. (Though its
   * a general scenario). The scenario is that the updates start coming 
   * before the base value reaches through GII. In that scenario the updates
   * essentially the deltas are added to a list and kept as oldValue in the
   * map and this method returns. When through GII the actual base value arrives
   * these updates or deltas are applied on it and the new value thus got is put
   * in the map.
   * @param event 
   * @param ifOld 
   * @return true if delta was enqued
   */
  private boolean enqueDelta(EntryEventImpl event, boolean ifOld) {
    final IndexUpdater indexManager = getIndexUpdater();
    LocalRegion owner = _getOwner();
    if (indexManager != null && !owner.isInitialized() && event.hasDelta()) {
      boolean isOldValueDelta = true;
      try {
        if (ifOld) {
          final Delta delta = event.getDeltaNewValue();
		  RegionEntry re = getOrCreateRegionEntry(owner, event, null,
          	  listOfDeltasCreator, false, false);
          assert re != null;
          synchronized (re) {
            @Retained @Released Object oVal = re.getValueOffHeapOrDiskWithoutFaultIn(owner);
            if (oVal != null) {
              try {
              if (oVal instanceof ListOfDeltas) {
                if (logger.isDebugEnabled()) {
                  logger.debug("basicPut: adding delta to list of deltas: {}", delta);
                }
                ((ListOfDeltas)oVal).merge(delta);
                @Retained Object newVal = ((AbstractRegionEntry)re).prepareValueForCache(owner, oVal, true);              
                re.setValue(owner, newVal); // TODO:KIRK:48068 prevent orphan
              }
              else {
                isOldValueDelta = false;
              }
              }finally {
                OffHeapHelper.release(oVal);
              }
            }
            else {
              if (logger.isDebugEnabled()) {
                logger.debug("basicPut: new list of deltas with delta: {}", delta);
              }
              @Retained Object newVal = new ListOfDeltas(delta);
              // TODO no need to call AbstractRegionMap.prepareValueForCache here?
              newVal = ((AbstractRegionEntry)re).prepareValueForCache(owner, newVal, true);
              re.setValue(owner, newVal); // TODO:KIRK:48068 prevent orphan
            }
          }
        }
      } catch (RegionClearedException ex) {
        // Neeraj: We can just ignore this exception because we are returning after this block
      }
      if (isOldValueDelta) {
        return true;
      }
    }
    return false;
  }

  /*
   * returns null if the operation fails
   */
  public RegionEntry basicPut(EntryEventImpl event,
                                    final long lastModified,
                                    final boolean ifNew,
                                    final boolean ifOld,
                                    Object expectedOldValue, // only non-null if ifOld
                                    boolean requireOldValue,
                                    final boolean overwriteDestroyed)
  throws CacheWriterException,
        TimeoutException {
    final LocalRegion owner = _getOwner();
    boolean clearOccured = false;
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this
          + " is null for event " + event);
    }
    if (logger.isTraceEnabled(LogMarker.LRU_TOMBSTONE_COUNT) && !(owner instanceof HARegion)) {
      logger.trace(LogMarker.LRU_TOMBSTONE_COUNT,
          "ARM.basicPut called for {} expectedOldValue={} requireOldValue={} ifNew={} ifOld={} initialized={} overwriteDestroyed={}",
          event, expectedOldValue, requireOldValue, ifNew, ifOld, owner.isInitialized(), overwriteDestroyed);
    }
    
    RegionEntry result = null;
    long lastModifiedTime = 0;
    // copy into local var to prevent race condition with setter
    final CacheWriter cacheWriter = owner.basicGetWriter();
    final boolean cacheWrite = !event.isOriginRemote() && !event.isNetSearch() && event.isGenerateCallbacks()
        && (cacheWriter != null
            || owner.hasServerProxy()
            || owner.scope.isDistributed());
    /*
     * For performance reason, we try to minimize object creation and do as much
     * work as we can outside of synchronization, especially getting
     * distribution advice.
     */
    final Set netWriteRecipients;
    if (cacheWrite) {
      if (cacheWriter == null && owner.scope.isDistributed()) {
        netWriteRecipients = ((DistributedRegion)owner)
            .getCacheDistributionAdvisor().adviseNetWrite();
      }
      else {
        netWriteRecipients = null;
      }
    }
    else {
      netWriteRecipients = null;
    }

    // mbid: this has been added to maintain consistency between the disk region
    // and the region map after clear() has been called. This will set the
    // reference of the diskSegmentRegion as a ThreadLocal so that if the diskRegionSegment
    // is later changed by another thread, we can do the necessary.
    boolean uninitialized = !owner.isInitialized();
    // SqlFabric Changes - BEGIN
    if (enqueDelta(event, ifOld)) {
      return null;
    }

    final IndexUpdater indexManager = getIndexUpdater();

    boolean sqlfIndexLocked = false;
    // SqlFabric Changes - END

    boolean retrieveOldValueForDelta = event.getDeltaBytes() != null
        && event.getRawNewValue() == null;
    lockForCacheModification(owner, event);
    IndexManager oqlIndexManager = null;
    try {
      // take read lock for SQLF index initializations if required; the index
      // GII lock is for any updates that may come in while index is being
      // loaded during replay see bug #41377; this will go away once we allow
      // for indexes to be loaded completely in parallel (#40899); need to
      // take this lock before the RegionEntry lock else a deadlock can happen
      // between this thread and index loading thread that will first take the
      // corresponding write lock on the IndexUpdater
      if (indexManager != null) {
        sqlfIndexLocked = indexManager.lockForIndexGII();
      }
      // Fix for Bug #44431. We do NOT want to update the region and wait
      // later for index INIT as region.clear() can cause inconsistency if
      // happened in parallel as it also does index INIT.
      oqlIndexManager = owner.getIndexManager() ; 
      if (oqlIndexManager != null) {
        oqlIndexManager.waitForIndexInit();
      }

      // fix for bug #42169, replace must go to server if entry not on client
      boolean replaceOnClient = event.getOperation() == Operation.REPLACE
                && owner.getServerProxy() != null; 
        // Rather than having two different blocks for synchronizing oldRe
        // and newRe, have only one block and synchronize re
        RegionEntry re = null;
        boolean eventRecorded = false;
        boolean onlyExisting = ifOld && !replaceOnClient;
		re = getOrCreateRegionEntry(owner, event, 
		    Token.REMOVED_PHASE1, null, onlyExisting, false);
        if (re == null) {
          throwExceptionForSqlFire(event);
          return null;
        }
        while (true) {
          synchronized (re) {
            // if the re goes into removed2 state, it will be removed
            // from the map. otherwise we can append an event to it
            // and change its state
            if (re.isRemovedPhase2()) {
                re = getOrCreateRegionEntry(owner, event,
                    Token.REMOVED_PHASE1, null, onlyExisting, false);
                _getOwner().getCachePerfStats().incRetries();
              if (re == null) {
                // this will happen when onlyExisting is true
                throwExceptionForSqlFire(event);
                return null;
              }
              continue;
            } else {
              @Released Object oldValueForDelta = null;
              if (retrieveOldValueForDelta) {
                // defer the lruUpdateCallback to prevent a deadlock (see bug 51121).
                final boolean disabled = disableLruUpdateCallback();
                try {
                  // Old value is faulted in from disk if not found in memory.
                  oldValueForDelta = re.getValue(owner); // OFFHEAP: if we are synced on oldRe no issue since we can use ARE's ref
                } finally {
                  if (disabled) {
                    enableLruUpdateCallback();
                  }
                }
              }

              try {

                event.setRegionEntry(re);
                // set old value in event
                setOldValueInEvent(event, re, cacheWrite, requireOldValue);
                if (!continueUpdate(re, event, ifOld, replaceOnClient)) {
                  return null;
                }
                // overwrite destroyed?
                if (!continueOverwriteDestroyed(re, event, overwriteDestroyed, ifNew)) {
                  return null;
                }
                // check expectedOldValue
                if (!satisfiesExpectedOldValue(event, re, expectedOldValue, replaceOnClient)) {
                  return null;
                }
                // invoke cacheWriter
                invokeCacheWriter(re, event, cacheWrite, cacheWriter,
                    netWriteRecipients, requireOldValue, expectedOldValue, replaceOnClient);

                // notify index of an update
                notifyIndex(re, true);
                  try {
                    try {
                      if ((cacheWrite && event.getOperation().isUpdate()) // if there is a cacheWriter, type of event has already been set
                          || !re.isRemoved()
                          || replaceOnClient) {
                        // update
                        updateEntry(event, requireOldValue, oldValueForDelta, re);
                      } else {
                        // create
                        createEntry(event, owner, re);
                      }
                      owner.recordEvent(event);
                      eventRecorded = true;
                    } catch (RegionClearedException rce) {
                      clearOccured = true;
                      owner.recordEvent(event);
                    } catch (ConcurrentCacheModificationException ccme) {
                      VersionTag tag = event.getVersionTag();
                      if (tag != null && tag.isTimeStampUpdated()) {
                        // Notify gateways of new time-stamp.
                        owner.notifyTimestampsToGateways(event);
                      }
                      throw ccme;
                    }
                    if (uninitialized) {
                      event.inhibitCacheListenerNotification(true);
                    }
                    updateLru(clearOccured, re, event);

                    lastModifiedTime = owner.basicPutPart2(event, re,
                        !uninitialized, lastModifiedTime, clearOccured);
                  } finally {
                    notifyIndex(re, false);
                  }
                  result = re;
                  break;
                } finally {
                  OffHeapHelper.release(oldValueForDelta);
                  if (re != null && !onlyExisting && !isOpComplete(re, event)) {
                    owner.cleanUpOnIncompleteOp(event, re, eventRecorded,
                        false/* updateStats */, replaceOnClient);
                  }
                  else if (re != null && owner.isUsedForPartitionedRegionBucket()) {
                  BucketRegion br = (BucketRegion)owner;
                  CachePerfStats stats = br.getPartitionedRegion().getCachePerfStats();
                  long startTime= stats.startCustomEviction();
                  CustomEvictionAttributes csAttr = br.getCustomEvictionAttributes();
                  // No need to update indexes if entry was faulted in but operation did not succeed. 
                  if (csAttr != null && (csAttr.isEvictIncoming() || re.isMarkedForEviction())) {
                    
                    if (csAttr.getCriteria().doEvict(event)) {
                      stats.incEvictionsInProgress();
                      // set the flag on event saying the entry should be evicted 
                      // and not indexed
                      EntryEventImpl destroyEvent = EntryEventImpl.create (owner, Operation.DESTROY, event.getKey(),
                          null/* newValue */, null, false, owner.getMyId());
                      try {

                      destroyEvent.setOldValueFromRegion();
                      destroyEvent.setCustomEviction(true);
                      destroyEvent.setPossibleDuplicate(event.isPossibleDuplicate());
                      if(logger.isDebugEnabled()) {
                        logger.debug("Evicting the entry " + destroyEvent);
                      }
                      if(result != null) {
                        removeEntry(event.getKey(),re, true, destroyEvent,owner, indexUpdater);
                      }
                      else{
                        removeEntry(event.getKey(),re, true, destroyEvent,owner, null);
                      }
                      //mark the region entry for this event as evicted 
                      event.setEvicted();
                      stats.incEvictions();
                      if(logger.isDebugEnabled()) {
                        logger.debug("Evicted the entry " + destroyEvent);
                      }
                      //removeEntry(event.getKey(), re);
                      } finally {
                        destroyEvent.release();
                        stats.decEvictionsInProgress();
                      }
                    } else {
                      re.clearMarkedForEviction();
                    }
                  }
                  stats.endCustomEviction(startTime);
                }
              } // try
            }
          } // sync re
        }// end while
    } catch (DiskAccessException dae) {
      //Asif:Feel that it is safe to destroy the region here as there appears
      // to be no chance of deadlock during region destruction      
      result = null;
      this._getOwner().handleDiskAccessException(dae);
      throw dae;
    } finally {
        releaseCacheModificationLock(owner, event);
        if (sqlfIndexLocked) {
          indexManager.unlockForIndexGII();
        }
        if (oqlIndexManager != null) {
          oqlIndexManager.countDownIndexUpdaters();
        }
        if (result != null) {
          try {
            // Note we do distribution after releasing all sync to avoid deadlock
            final boolean invokeListeners = event.basicGetNewValue() != Token.TOMBSTONE;
            owner.basicPutPart3(event, result, !uninitialized,
                lastModifiedTime, invokeListeners, ifNew, ifOld, expectedOldValue, requireOldValue);
          } catch (EntryExistsException eee) {
            // SQLFabric changes BEGIN
            // ignore EntryExistsException in distribution from a non-empty
            // region since actual check will be done in this put itself
            // and it can happen in distribution if put comes in from
            // GII as well as distribution channel
            if (indexManager != null) {
              if (logger.isTraceEnabled()) {
                logger.trace("basicPut: ignoring EntryExistsException in distribution {}", eee);
              }
            }
            else {
              // can this happen for non-SQLFabric case?
              throw eee;
            }
            // SQLFabric changes END
          } finally {
            // bug 32589, post update may throw an exception if exception occurs
            // for any recipients
            if (!clearOccured) {
              try {
                lruUpdateCallback();
              } catch( DiskAccessException dae) {
                //Asif:Feel that it is safe to destroy the region here as there appears
                // to be no chance of deadlock during region destruction      
                result = null;
                this._getOwner().handleDiskAccessException(dae);
                throw dae;
              }
            }
          } //  finally
        } else {
          resetThreadLocals();
        }
    } // finally
    
    return result;
  }

  /**
   * If the value in the VM is still REMOVED_PHASE1 Token, then the operation
   * was not completed (due to cacheWriter exception, concurrentMap operation) etc.
   */
  private boolean isOpComplete(RegionEntry re, EntryEventImpl event) {
    if (re.getValueAsToken() == Token.REMOVED_PHASE1) {
      return false;
    }
    return true;
  }

  private boolean satisfiesExpectedOldValue(EntryEventImpl event,
      RegionEntry re, Object expectedOldValue, boolean replaceOnClient) {
    // replace is propagated to server, so no need to check
    // satisfiesOldValue on client
    if (expectedOldValue != null && !replaceOnClient) {
      ReferenceCountHelper.skipRefCountTracking();
      
      @Retained @Released Object v = re._getValueRetain(event.getLocalRegion(), true);
      
      ReferenceCountHelper.unskipRefCountTracking();
      try {
        if (!AbstractRegionEntry.checkExpectedOldValue(expectedOldValue, v, event.getLocalRegion())) {
          return false;
        }
      } finally {
        OffHeapHelper.releaseWithNoTracking(v);
      }
    }
    return true;
  }

  // Asif: If the new value is an instance of SerializableDelta, then
  // the old value requirement is a must & it needs to be faulted in
  // if overflown to disk without affecting LRU? This is needed for
  // Sql Fabric.
  // [sumedh] store both the value in VM and the value in VM or disk;
  // the former is used for updating the VM size calculations, while
  // the latter is used in other places like passing to
  // SqlfIndexManager or setting the old value in the event; this is
  // required since using the latter for updating the size
  // calculations will be incorrect in case the value was read from
  // disk but not brought into the VM like what getValueInVMOrDisk
  // method does when value is not found in VM
  // PRECONDITION: caller must be synced on re
  private void setOldValueInEvent(EntryEventImpl event, RegionEntry re, boolean cacheWrite, boolean requireOldValue) {
    boolean needToSetOldValue = getIndexUpdater() != null || cacheWrite || requireOldValue || event.getOperation().guaranteesOldValue();
    if (needToSetOldValue) {
      if (event.hasDelta() || event.getOperation().guaranteesOldValue()
          || GemFireCacheImpl.sqlfSystem()) {
        // In these cases we want to even get the old value from disk if it is not in memory
        ReferenceCountHelper.skipRefCountTracking();
        @Released Object oldValueInVMOrDisk = re.getValueOffHeapOrDiskWithoutFaultIn(event.getLocalRegion());
        ReferenceCountHelper.unskipRefCountTracking();
        try {
          event.setOldValue(oldValueInVMOrDisk, requireOldValue
              || GemFireCacheImpl.sqlfSystem());
        } finally {
          OffHeapHelper.releaseWithNoTracking(oldValueInVMOrDisk);
        }
      } else {
        // In these cases only need the old value if it is in memory
        ReferenceCountHelper.skipRefCountTracking();
        
        @Retained @Released Object oldValueInVM = re._getValueRetain(event.getLocalRegion(), true); // OFFHEAP: re synced so can use its ref.
        
        ReferenceCountHelper.unskipRefCountTracking();
        try {
          event.setOldValue(oldValueInVM,
              requireOldValue || GemFireCacheImpl.sqlfSystem());
        } finally {
          OffHeapHelper.releaseWithNoTracking(oldValueInVM);
        }
      }
    } else {
      // if the old value is in memory then if it is a GatewaySenderEventImpl then
      // we want to set the old value.
      @Unretained Object ov = re._getValue(); // OFFHEAP _getValue is ok since re is synced and we only use it if its a GatewaySenderEventImpl.
      // Since GatewaySenderEventImpl is never stored in an off-heap region nor a compressed region we don't need to worry about ov being compressed.
      if (ov instanceof GatewaySenderEventImpl) {
        event.setOldValue(ov, true);
      }
    }
  }

  /**
   * Asif: If the system is sqlfabric and the event has delta, then re == null 
   * implies update on non existent row . Throwing ENFE in that case 
   * As  returning a boolean etc has other complications in terms of PR reattempt etc  
   */
  private void throwExceptionForSqlFire(EntryEventImpl event) {
    if (event.hasDelta() && _getOwner().getGemFireCache().isSqlfSystem()) {
      throw new EntryNotFoundException(
          "SqlFabric::No row found for update");
    }
  }

  protected void createEntry(EntryEventImpl event, final LocalRegion owner,
      RegionEntry re) throws RegionClearedException {
    final boolean wasTombstone = re.isTombstone();
    processVersionTag(re, event);
    event.putNewEntry(owner, re);
    updateSize(event, 0, false, wasTombstone);
    if (!event.getLocalRegion().isInitialized()) {
      owner.getImageState().removeDestroyedEntry(event.getKey());
    }
  }

  protected void updateEntry(EntryEventImpl event, boolean requireOldValue,
      Object oldValueForDelta, RegionEntry re) throws RegionClearedException {
    final int oldSize = event.getLocalRegion().calculateRegionEntryValueSize(re);
    final boolean wasTombstone = re.isTombstone();
    processVersionTag(re, event);
    event.putExistingEntry(event.getLocalRegion(), re, requireOldValue,
        oldValueForDelta);
    EntryLogger.logPut(event);
    updateSize(event, oldSize, true/* isUpdate */, wasTombstone);
  }

  private void updateLru(boolean clearOccured, RegionEntry re, EntryEventImpl event) {
    if (!clearOccured) {
      if (event.getOperation().isCreate()) {
        lruEntryCreate(re);
      } else {
        lruEntryUpdate(re);
      }
    }
  }

  private void updateSize(EntryEventImpl event, int oldSize, boolean isUpdate, boolean wasTombstone) {
    if (isUpdate && !wasTombstone) {
      _getOwner().updateSizeOnPut(event.getKey(), oldSize, event.getNewValueBucketSize());
    } else {
      _getOwner().updateSizeOnCreate(event.getKey(), event.getNewValueBucketSize());
      if (!wasTombstone) {
        incEntryCount(1);
      }
    }
  }

  private void notifyIndex(RegionEntry re, boolean isUpdating) {
    if (_getOwner().indexMaintenanceSynchronous) {
      re.setUpdateInProgress(isUpdating);
    }
  }

  private void invokeCacheWriter(RegionEntry re, EntryEventImpl event,
      boolean cacheWrite, CacheWriter cacheWriter, Set netWriteRecipients,
      boolean requireOldValue, Object expectedOldValue, boolean replaceOnClient) {
    // invoke listeners only if region is initialized
    if (_getOwner().isInitialized() && cacheWrite) {
      // event.setOldValue already called in setOldValueInEvent

      // bug #42638 for replaceOnClient, do not make the event create
      // or update since replace must propagate to server
      if (!replaceOnClient) {
        if (re.isDestroyedOrRemoved()) {
          event.makeCreate();
        } else {
          event.makeUpdate();
        }
      }
      _getOwner().cacheWriteBeforePut(event, netWriteRecipients, cacheWriter,
          requireOldValue, expectedOldValue);
    }
    if (!_getOwner().isInitialized() && !cacheWrite) {
      // block setting of old value in putNewValueNoSync, don't
      // need it
      event.oldValueNotAvailable();
    }
  }

  private boolean continueOverwriteDestroyed(RegionEntry re,
      EntryEventImpl event, boolean overwriteDestroyed, boolean ifNew) {
    Token oldValueInVM = re.getValueAsToken();
    // if region is under GII, check if token is destroyed
    if (!overwriteDestroyed) {
      if (!_getOwner().isInitialized() && (oldValueInVM == Token.DESTROYED || oldValueInVM == Token.TOMBSTONE)) {
        event.setOldValueDestroyedToken();
        return false;
      }
    }
    if (ifNew && !Token.isRemoved(oldValueInVM)) {
      return false;
    }
    return true;
  }

  private boolean continueUpdate(RegionEntry re, EntryEventImpl event,
      boolean ifOld, boolean replaceOnClient) {
    if (ifOld) {
      // only update, so just do tombstone maintainence and exit
      if (re.isTombstone() && event.getVersionTag() != null) {
        // refresh the tombstone so it doesn't time out too soon
        processVersionTag(re, event);
        try {
          re.setValue(_getOwner(), Token.TOMBSTONE);
        } catch (RegionClearedException e) {
          // that's okay - when writing a tombstone into a disk, the
          // region has been cleared (including this tombstone)
        }
        _getOwner().rescheduleTombstone(re, re.getVersionStamp().asVersionTag());
        return false;
      }
      if (re.isRemoved() && !replaceOnClient) {
        return false;
      }
    }
    return true;
  }

  protected boolean destroyEntry(RegionEntry re, EntryEventImpl event,
      boolean inTokenMode, boolean cacheWrite, @Released Object expectedOldValue,
      boolean forceDestroy, boolean removeRecoveredEntry)
      throws CacheWriterException, TimeoutException, EntryNotFoundException,
      RegionClearedException {
    processVersionTag(re, event);
    final int oldSize = _getOwner().calculateRegionEntryValueSize(re);
    boolean retVal = re.destroy(event.getLocalRegion(), event, inTokenMode,
        cacheWrite, expectedOldValue, forceDestroy, removeRecoveredEntry);
    if (retVal) {
      EntryLogger.logDestroy(event);
      _getOwner().updateSizeOnRemove(event.getKey(), oldSize);
    }
    return retVal;
  }

  public void txApplyPut(Operation p_putOp, Object key, Object nv,
      boolean didDestroy, TransactionId txId, TXRmtEvent txEvent, 
      EventID eventId, Object aCallbackArgument,List<EntryEventImpl> pendingCallbacks,FilterRoutingInfo filterRoutingInfo,ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag, long tailKey)
  {
    final LocalRegion owner = _getOwner();
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this
          + " is null");
    }
    
    Operation putOp = p_putOp;
    
    owner.checkBeforeEntrySync(txEvent);
    Object newValue = nv;

    final boolean hasRemoteOrigin = !((TXId)txId).getMemberId().equals(owner.getMyId());
    final boolean isTXHost = txEntryState != null;
    final boolean isClientTXOriginator = owner.cache.isClient() && !hasRemoteOrigin;
    final boolean isRegionReady = owner.isInitialized();
    EntryEventImpl cbEvent = null;
    EntryEventImpl sqlfEvent = null;
    boolean invokeCallbacks = shouldCreateCBEvent(owner, false /*isInvalidate*/, isRegionReady);
    boolean cbEventInPending = false;
    cbEvent = createCBEvent(owner, putOp, key, newValue, txId, 
        txEvent, eventId, aCallbackArgument,filterRoutingInfo,bridgeContext, txEntryState, versionTag, tailKey);
    try {
    if (logger.isDebugEnabled()) {
      logger.debug("txApplyPut cbEvent={}", cbEvent);
    }
    
    
    if (owner.isUsedForPartitionedRegionBucket()) {
      newValue = EntryEventImpl.getCachedDeserializable(nv, cbEvent);
      txHandleWANEvent(owner, cbEvent, txEntryState);
    }
    
    if (/*owner.isUsedForPartitionedRegionBucket() && */ 
       (getIndexUpdater() != null ||
       (newValue instanceof com.gemstone.gemfire.internal.cache.delta.Delta))) {
      sqlfEvent = createCBEvent(owner, putOp, key, newValue, txId, 
          txEvent, eventId, aCallbackArgument,filterRoutingInfo,bridgeContext, txEntryState, versionTag, tailKey);
    }
    boolean opCompleted = false;
    // Fix for Bug #44431. We do NOT want to update the region and wait
    // later for index INIT as region.clear() can cause inconsistency if
    // happened in parallel as it also does index INIT.
    IndexManager oqlIndexManager = owner.getIndexManager() ; 
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    lockForTXCacheModification(owner, versionTag);
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
                  if (sqlfEvent != null) {
                    sqlfEvent.setOldValue(re.getValueInVM(owner)); // OFFHEAP eei
                  }

                  boolean clearOccured = false;
                  // Set RegionEntry updateInProgress
                  if (owner.indexMaintenanceSynchronous) {
                    re.setUpdateInProgress(true);
                  }
                  try {
                    txRemoveOldIndexEntry(putOp, re);
                    if (didDestroy) {
                      re.txDidDestroy(owner.cacheTimeMillis());
                    }
                    if (txEvent != null) {
                      txEvent.addPut(putOp, owner, re, re.getKey(), newValue,aCallbackArgument);
                    }
                    re.setValueResultOfSearch(putOp.isNetSearch());
                    try {
                      // Rahul: applies the delta and sets the new value in 
                      // region entry (required for sqlfabric delta).
                      processAndGenerateTXVersionTag(owner, cbEvent, re, txEntryState);
                      if (newValue instanceof com.gemstone.gemfire.internal.cache.delta.Delta 
                          && sqlfEvent != null) {
                        //cbEvent.putExistingEntry(owner, re);
                        sqlfEvent.putExistingEntry(owner, re);
                      } else {
                        re.setValue(owner, re.prepareValueForCache(owner, newValue, cbEvent, !putOp.isCreate()));
                      }
                      if (putOp.isCreate()) {
                        owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(re));
                      } else if (putOp.isUpdate()) {
                       // Rahul : fix for 41694. Negative bucket size can also be 
                        // an issue with normal GFE Delta and will have to be fixed 
                        // in a similar manner and may be this fix the the one for 
                        // other delta can be combined.
                        if (sqlfEvent != null) {
                          owner.updateSizeOnPut(key, oldSize, sqlfEvent.getNewValueBucketSize());
                        } else {
                          owner.updateSizeOnPut(key, oldSize, owner.calculateRegionEntryValueSize(re));
                        }
                      }
                    }
                    catch (RegionClearedException rce) {
                      clearOccured = true;
                    } 
                    {
                      long lastMod = owner.cacheTimeMillis();
                      EntryLogger.logTXPut(_getOwnerObject(), key, nv);
                      re.updateStatsForPut(lastMod);
                      owner.txApplyPutPart2(re, re.getKey(), newValue, lastMod,
                          false, didDestroy, clearOccured);
                    }
                  } finally {
                    if (re != null && owner.indexMaintenanceSynchronous) {
                      re.setUpdateInProgress(false);
                    }
                  }
                  if (invokeCallbacks) {
                    cbEvent.makeUpdate();
                    switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                    if(pendingCallbacks==null) {
                      owner.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE,
                          cbEvent, hasRemoteOrigin);
                    } else {
                      pendingCallbacks.add(cbEvent);
                      cbEventInPending = true;
                    }
                  }
                  if (!clearOccured) {
                    lruEntryUpdate(re);
                  }
                }
              }
            if (didDestroy && !opCompleted) {
              owner
                  .txApplyInvalidatePart2(re, re.getKey(), true, false, false /* clear*/);
            }
          }
          if (owner.concurrencyChecksEnabled && txEntryState != null && cbEvent != null) {
            txEntryState.setVersionTag(cbEvent.getVersionTag());
          }
          return;
        }
      }
      RegionEntry newRe = getEntryFactory().createEntry(owner, key,
          Token.REMOVED_PHASE1);
        synchronized (newRe) {
          try {
            RegionEntry oldRe = putEntryIfAbsent(key, newRe);
            while (!opCompleted && oldRe != null) {
              synchronized (oldRe) {
                if (oldRe.isRemovedPhase2()) {
                  oldRe = putEntryIfAbsent(key, newRe);
                  if (oldRe != null) {
                    owner.getCachePerfStats().incRetries();
                  }
                }
                else {
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
                  if (sqlfEvent != null) {
                    sqlfEvent.setOldValue(oldRe.getValueInVM(owner)); // OFFHEAP eei
                  }
                  boolean clearOccured = false;
                  // Set RegionEntry updateInProgress
                  if (owner.indexMaintenanceSynchronous) {
                    oldRe.setUpdateInProgress(true);
                  }
                  try {
                    txRemoveOldIndexEntry(putOp, oldRe);
                    if (didDestroy) {
                      oldRe.txDidDestroy(owner.cacheTimeMillis());
                    }
                    if (txEvent != null) {
                      txEvent.addPut(putOp, owner, oldRe, oldRe.getKey(), newValue,aCallbackArgument);
                    }
                    oldRe.setValueResultOfSearch(putOp.isNetSearch());
                    try {
                      processAndGenerateTXVersionTag(owner, cbEvent, oldRe, txEntryState);
                      boolean wasTombstone = oldRe.isTombstone();
                      if (newValue instanceof com.gemstone.gemfire.internal.cache.delta.Delta 
                          && sqlfEvent != null ) {
                        //cbEvent.putExistingEntry(owner, oldRe);
                        sqlfEvent.putExistingEntry(owner, oldRe);
                      } else {
                        oldRe.setValue(owner, oldRe.prepareValueForCache(owner, newValue, cbEvent, !putOp.isCreate()));
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
                        if (sqlfEvent != null) {
                          owner.updateSizeOnPut(key, oldSize, sqlfEvent.getNewValueBucketSize());
                        } else {
                          owner.updateSizeOnPut(key, oldSize, owner.calculateRegionEntryValueSize(oldRe));
                        }
                      }
                    }
                    catch (RegionClearedException rce) {
                      clearOccured = true;
                    }
                    {
                      long lastMod = System.currentTimeMillis();
                      EntryLogger.logTXPut(_getOwnerObject(), key, nv);
                      oldRe.updateStatsForPut(lastMod);
                      owner.txApplyPutPart2(oldRe, oldRe.getKey(), newValue,
                          lastMod, false, didDestroy, clearOccured);
                    }
                  } finally {
                    if (oldRe != null && owner.indexMaintenanceSynchronous) {
                      oldRe.setUpdateInProgress(false);
                    }
                  }
                  if (invokeCallbacks) {
                    if (!oldIsRemoved) {
                      cbEvent.makeUpdate();
                    }
                    switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                    if(pendingCallbacks==null) {
                      owner.invokeTXCallbacks(cbEvent.op.isCreate() ? EnumListenerEvent.AFTER_CREATE : EnumListenerEvent.AFTER_UPDATE,
                          cbEvent, true/*callDispatchListenerEvent*/);
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
              if (owner.indexMaintenanceSynchronous) {
                newRe.setUpdateInProgress(true);
              }
              try {
                txRemoveOldIndexEntry(putOp, newRe);
                // creating a new entry
                if (didDestroy) {
                  newRe.txDidDestroy(owner.cacheTimeMillis());
                }
                if (txEvent != null) {
                  txEvent.addPut(putOp, owner, newRe, newRe.getKey(), newValue,aCallbackArgument);
                }
                newRe.setValueResultOfSearch(putOp.isNetSearch());
                try {
                  
                  processAndGenerateTXVersionTag(owner, cbEvent, newRe, txEntryState);
                  if (sqlfEvent != null ) {
                    sqlfEvent.putNewEntry(owner,newRe);
                  } else {
                    newRe.setValue(owner, newRe.prepareValueForCache(owner, newValue, cbEvent, !putOp.isCreate()));
                  }
                  owner.updateSizeOnCreate(newRe.getKey(), owner.calculateRegionEntryValueSize(newRe));
                }
                catch (RegionClearedException rce) {
                  clearOccured = true;
                }
                {
                  long lastMod = System.currentTimeMillis();
                  EntryLogger.logTXPut(_getOwnerObject(), key, nv);
                  newRe.updateStatsForPut(lastMod);
                  owner.txApplyPutPart2(newRe, newRe.getKey(), newValue, lastMod,
                      true, didDestroy, clearOccured);
                }
              } finally {
                if (newRe != null && owner.indexMaintenanceSynchronous) {
                  newRe.setUpdateInProgress(false);
                }
              }
              opCompleted = true;
              if (invokeCallbacks) {
                cbEvent.makeCreate();
                cbEvent.setOldValue(null);
                switchEventOwnerAndOriginRemote(cbEvent, hasRemoteOrigin);
                if(pendingCallbacks==null) {
                  owner.invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, cbEvent,
                      true/*callDispatchListenerEvent*/);
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
          }
          finally {
            if (!opCompleted) {
              removeEntry(key, newRe, false);
            }
          }
        }
      if (owner.concurrencyChecksEnabled && txEntryState != null && cbEvent != null) {
        txEntryState.setVersionTag(cbEvent.getVersionTag());
      }
    }catch( DiskAccessException dae) {
      owner.handleDiskAccessException(dae);
      throw dae;
    }
    finally {
      releaseTXCacheModificationLock(owner, versionTag);
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
    }
    } finally {
      if (!cbEventInPending) cbEvent.release();
      if (sqlfEvent != null) sqlfEvent.release();
    }
  }

  private void txHandleWANEvent(final LocalRegion owner, EntryEventImpl cbEvent, TXEntryState txEntryState) {
    ((BucketRegion)owner).handleWANEvent(cbEvent);
    if (txEntryState != null) {
      txEntryState.setTailKey(cbEvent.getTailKey());
    }
  }

  /**
   * called from txApply* methods to process and generate versionTags.
   */
  private void processAndGenerateTXVersionTag(final LocalRegion owner,
      EntryEventImpl cbEvent, RegionEntry re, TXEntryState txEntryState) {
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
      
      //cbEvent.setNextRegionVersion(txEntryState.getNextRegionVersion());
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
   * Switch the event's region from BucketRegion to owning PR and set originRemote to the given value
   */
  static EntryEventImpl switchEventOwnerAndOriginRemote(EntryEventImpl event, boolean originRemote) {
    assert event != null;
    if (event.getRegion().isUsedForPartitionedRegionBucket()) {
      LocalRegion pr = event.getRegion().getPartitionedRegion();
      event.setRegion(pr);
    }
    event.setOriginRemote(originRemote);
    return event;
  }

  /**
   * Removing the existing indexed value requires the current value in the cache, 
   * that is the one prior to applying the operation.
   * @param op
   * @param entry the RegionEntry that contains the value prior to applying the op
   */
  private void txRemoveOldIndexEntry(Operation op, RegionEntry entry) {
    if ((op.isUpdate() && !entry.isInvalid()) ||
        op.isInvalidate() || op.isDestroy()) {
      IndexManager idxManager = _getOwner().getIndexManager();
      if (idxManager != null) {
        try {
          idxManager.updateIndexes(entry,
                                  IndexManager.REMOVE_ENTRY,
                                  op.isUpdate() ?
                                  IndexProtocol.BEFORE_UPDATE_OP :
                                  IndexProtocol.OTHER_OP);
        } catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        }
      }
    }
  }
  
  public void dumpMap() {
    logger.debug("dump of concurrent map of size {} for region {}", this._getMap().size(), this._getOwner());
    for (Iterator it = this._getMap().values().iterator(); it.hasNext(); ) {
      logger.trace("dumpMap:"+it.next().toString());
    }
  }
  
  static boolean shouldCreateCBEvent( final LocalRegion owner, 
      final boolean isInvalidate, final boolean isInitialized)
  {
    LocalRegion lr = owner;
    boolean isPartitioned = lr.isUsedForPartitionedRegionBucket();
    
    if(isPartitioned){
     /* if(!((BucketRegion)lr).getBucketAdvisor().isPrimary()) {
        if(!BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION) {
          return false;
        }
      }*/
      lr = owner.getPartitionedRegion();
    }
    if (isInvalidate) { // ignore shouldNotifyGatewayHub check for invalidates
      return (isPartitioned || isInitialized)
          && (lr.shouldDispatchListenerEvent()
            || lr.shouldNotifyBridgeClients()
            || lr.getConcurrencyChecksEnabled());
    } else {
      return (isPartitioned || isInitialized)
          && (lr.shouldDispatchListenerEvent()
            || lr.shouldNotifyBridgeClients()
            || lr.getConcurrencyChecksEnabled());
    }
  }

  /** create a callback event for applying a transactional change to the local cache */
  public static final EntryEventImpl createCBEvent(final LocalRegion re,
      Operation op, Object key, Object newValue, TransactionId txId, 
      TXRmtEvent txEvent,EventID eventId, Object aCallbackArgument,FilterRoutingInfo filterRoutingInfo,ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag, long tailKey)
  {
    DistributedMember originator = null ;
    //txId should not be null even on localOrigin
    Assert.assertTrue(txId != null);
    originator = ((TXId)txId).getMemberId();
    
    LocalRegion eventRegion = re;
    if (eventRegion.isUsedForPartitionedRegionBucket()) {
      eventRegion = re.getPartitionedRegion();
    }
    
    EntryEventImpl retVal = EntryEventImpl.create(
        re, op, key, newValue,
        aCallbackArgument,
        txEntryState == null, originator);
    boolean returnedRetVal = false;
    try {
 
    
    if(bridgeContext!=null) {
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
        localRouting = filterRoutingInfo.getFilterInfo(re.getMyId());
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
      logger.trace("createCBEvent filterRouting={} computeFilterInfo={} local routing={}", filterRoutingInfo, computeFilterInfo, localRouting);
    }
    
    if (re.isUsedForPartitionedRegionBucket()) {
      BucketRegion bucket = (BucketRegion)re;
      if(BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION || bucket.getBucketAdvisor().isPrimary()) {
        retVal.setInvokePRCallbacks(true);
      } else {
        retVal.setInvokePRCallbacks(false);
      }

      if (computeFilterInfo) {
        if (bucket.getBucketAdvisor().isPrimary()) {
          if (logger.isTraceEnabled()) {
            logger.trace("createCBEvent computing routing for primary bucket");
          }
          FilterProfile fp = ((BucketRegion)re).getPartitionedRegion().getFilterProfile();
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
      FilterProfile fp = re.getFilterProfile();
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

  public final void writeSyncIfPresent(Object key, Runnable runner)
  {
    RegionEntry re = getEntry(key);
    if (re != null) {
      final boolean disabled = disableLruUpdateCallback();
      try {
        synchronized (re) {
          if (!re.isRemoved()) {
            runner.run();
          }
        }
      }
      finally {
        if (disabled) {
          enableLruUpdateCallback();
        }
        try {
          lruUpdateCallback();
        }catch(DiskAccessException dae) {
          this._getOwner().handleDiskAccessException(dae);
          throw dae;
        }
      }
    }
  }

  public final void removeIfDestroyed(Object key)
  {
    LocalRegion owner = _getOwner();
//    boolean makeTombstones = owner.concurrencyChecksEnabled;
    DiskRegion dr = owner.getDiskRegion();
      RegionEntry re = getEntry(key);
      if (re != null) {
        if (re.isDestroyed()) {
          synchronized (re) {
            if (re.isDestroyed()) {
              // [bruce] destroyed entries aren't in the LRU clock, so they can't be retained here
//              if (makeTombstones) {
//                re.makeTombstone(owner, re.getVersionStamp().asVersionTag());
//              } else {
              re.removePhase2();
              removeEntry(key, re, true);
            }
          }
        }
      }
//      }
  }
  

  /** get version-generation permission from the region's version vector */
  private void lockForCacheModification(LocalRegion owner, EntryEventImpl event) {
    boolean lockedByBulkOp = event.isBulkOpInProgress() && owner.dataPolicy.withReplication();
    if (!event.isOriginRemote() && !lockedByBulkOp) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.lockForCacheModification(owner);
      }
    }
  }
  
  /** release version-generation permission from the region's version vector */
  private void releaseCacheModificationLock(LocalRegion owner, EntryEventImpl event) {
    boolean lockedByBulkOp = event.isBulkOpInProgress() && owner.dataPolicy.withReplication();
    if (!event.isOriginRemote() && !lockedByBulkOp) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.releaseCacheModificationLock(owner);
      }
    }
  }
  
  /** get version-generation permission from the region's version vector */
  private void lockForTXCacheModification(LocalRegion owner, VersionTag tag) {
    if ( !(tag != null && tag.isFromOtherMember()) ) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.lockForCacheModification(owner);
      }
    }
  }
  
  /** release version-generation permission from the region's version vector */
  private void releaseTXCacheModificationLock(LocalRegion owner, VersionTag tag) {
    if ( !(tag != null && tag.isFromOtherMember()) ) {
      RegionVersionVector vector = owner.getVersionVector();
      if (vector != null) {
        vector.releaseCacheModificationLock(owner);
      }
    }
  }

  public final void unscheduleTombstone(RegionEntry re) {
  }
  
  /**
   * for testing race conditions between threads trying to apply ops to the
   * same entry
   * @param entry the entry to attempt to add to the system
   */
  protected final RegionEntry putEntryIfAbsentForTest(RegionEntry entry) {
    return (RegionEntry)putEntryIfAbsent(entry.getKey(), entry);
  }

  public boolean isTombstoneNotNeeded(RegionEntry re, int destroyedVersion) {
    // no need for synchronization - stale values are okay here
    RegionEntry actualRe = getEntry(re.getKey());
    // TODO this looks like a problem for regionEntry pooling
    if (actualRe != re) {  // null actualRe is okay here
      return true; // tombstone was evicted at some point
    }
    VersionStamp vs = re.getVersionStamp();
    if (vs == null) {
      // if we have no VersionStamp why were we even added as a tombstone?
      // We used to see an NPE here. See bug 52092.
      logger.error("Unexpected RegionEntry scheduled as tombstone: re.getClass {} destroyedVersion {}", re.getClass(), destroyedVersion);
      return true;
    }
    int entryVersion = vs.getEntryVersion();
    boolean isSameTombstone = (entryVersion == destroyedVersion && re.isTombstone());
    return !isSameTombstone;
  }

  /** removes a tombstone that has expired locally */
  public final boolean removeTombstone(RegionEntry re, VersionHolder version, boolean isEviction, boolean isScheduledTombstone)  {
    boolean result = false;
    int destroyedVersion = version.getEntryVersion();

    synchronized(this._getOwner().getSizeGuard()) { // do this sync first; see bug 51985
        synchronized (re) {
          int entryVersion = re.getVersionStamp().getEntryVersion();
          boolean isTombstone = re.isTombstone();
          boolean isSameTombstone = (entryVersion == destroyedVersion && isTombstone);
          if (isSameTombstone || (isTombstone && entryVersion < destroyedVersion)) {
            if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT)) {
              // logs are at info level for TomstoneService.DEBUG_TOMBSTONE_COUNT so customer doesn't have to use fine level
              if (isSameTombstone) {
                // logging this can put tremendous pressure on the log writer in tests
                // that "wait for silence"
                logger.trace(LogMarker.TOMBSTONE_COUNT,
                    "removing tombstone for {} with v{} rv{}; count is {}",
                    re.getKey(), destroyedVersion, version.getRegionVersion(), (this._getOwner().getTombstoneCount() - 1));
              } else {
                logger.trace(LogMarker.TOMBSTONE_COUNT, "removing entry (v{}) that is older than an expiring tombstone (v{} rv{}) for {}",
                    entryVersion, destroyedVersion, version.getRegionVersion(), re.getKey());
              }
            }
            try {
              re.setValue(_getOwner(), Token.REMOVED_PHASE2);
              if (removeTombstone(re)) {
                result = true;
                incEntryCount(-1);
                // Bug 51118: When the method is called by tombstoneGC thread, current 're' is an
                // expired tombstone. Then we detected an destroyed (due to overwritingOldTombstone() 
                // returns true earlier) tombstone with bigger entry version, it's safe to delete
                // current tombstone 're' and adjust the tombstone count. 
  //              lruEntryDestroy(re); // tombstones are invisible to LRU
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
              //if the region has been destroyed, the tombstone is already
              //gone. Catch an exception to avoid an error from the GC thread.
            }
          } else {
            if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT)) {
              logger.trace(LogMarker.TOMBSTONE_COUNT,
                  "tombstone for {} was resurrected with v{}; destroyed version was v{}; count is {}; entryMap size is {}",
                  re.getKey(), re.getVersionStamp().getEntryVersion(), destroyedVersion, this._getOwner().getTombstoneCount(), size());
            }
          }
        }
      }
    return result;
  }

  protected boolean removeTombstone(RegionEntry re) {
    return _getMap().remove(re.getKey(), re);
  }

  // method used for debugging tombstone count issues
  public boolean verifyTombstoneCount(AtomicInteger numTombstones) {
    int deadEntries = 0;
    try {
      for (Iterator it=_getMap().values().iterator(); it.hasNext(); ) {
        RegionEntry re = (RegionEntry)it.next();
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
}

