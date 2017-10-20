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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.AbstractRegionMap.ARMLockTestHook;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.lru.LRUEntry;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * Internal implementation of {@link RegionMap}for regions whose DataPolicy is proxy. Proxy maps are
 * always empty.
 * 
 * @since GemFire 5.0
 */
class ProxyRegionMap implements RegionMap {

  protected ProxyRegionMap(LocalRegion owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    this.owner = owner;
    this.attr = attr;
  }

  /**
   * the region that owns this map
   */
  private final LocalRegion owner;

  private final Attributes attr;

  public RegionEntryFactory getEntryFactory() {
    throw new UnsupportedOperationException();
  }

  public Attributes getAttributes() {
    return this.attr;
  }

  public void setOwner(Object r) {
    throw new UnsupportedOperationException();
  }

  public void changeOwner(LocalRegion r) {
    throw new UnsupportedOperationException();
  }

  public int size() {
    return 0;
  }

  public boolean isEmpty() {
    return true;
  }

  public Set keySet() {
    return Collections.EMPTY_SET;
  }

  public Collection<RegionEntry> regionEntries() {
    return Collections.emptySet();
  }

  @Override
  public Collection<RegionEntry> regionEntriesInVM() {
    return Collections.emptySet();
  }

  public boolean containsKey(Object key) {
    return false;
  }

  public RegionEntry getEntry(Object key) {
    return null;
  }

  public RegionEntry putEntryIfAbsent(Object key, RegionEntry re) {
    return null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Set<VersionSource> clear(RegionVersionVector rvv) {
    // nothing needs to be done
    RegionVersionVector v = this.owner.getVersionVector();
    if (v != null) {
      return v.getDepartedMembersSet();
    } else {
      return Collections.emptySet();
    }
  }

  public void diskClear() {
    // nothing needs to be done
  }

  public RegionEntry initRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    throw new UnsupportedOperationException();
  }

  public RegionEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Used to modify an existing RegionEntry or create a new one when processing the values obtained
   * during a getInitialImage.
   */
  public boolean initialImagePut(Object key, long lastModified, Object newValue,
      boolean wasRecovered, boolean deferLRUCallback, VersionTag version,
      InternalDistributedMember sender, boolean forceValue) {
    throw new UnsupportedOperationException();
  }

  public boolean destroy(EntryEventImpl event, boolean inTokenMode, boolean duringRI,
      boolean cacheWrite, boolean isEviction, Object expectedOldValue, boolean removeRecoveredEntry)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    if (event.getOperation().isLocal()) {
      throw new EntryNotFoundException(event.getKey().toString());
    }
    if (cacheWrite) {
      this.owner.cacheWriteBeforeDestroy(event, expectedOldValue);
    }
    owner.recordEvent(event);
    this.owner.basicDestroyPart2(markerEntry, event, inTokenMode,
        false /* Clear conflict occurred */, duringRI, true);
    this.owner.basicDestroyPart3(markerEntry, event, inTokenMode, duringRI, true, expectedOldValue);
    return true;
  }

  public boolean invalidate(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry,
      boolean forceCallback) throws EntryNotFoundException {

    if (event.getOperation().isLocal()) {
      if (this.owner.isInitialized()) {
        AbstractRegionMap.forceInvalidateEvent(event, this.owner);
      }
      throw new EntryNotFoundException(event.getKey().toString());
    }
    this.owner.serverInvalidate(event);
    this.owner.recordEvent(event);
    this.owner.basicInvalidatePart2(markerEntry, event, false /* Clear conflict occurred */, true);
    this.owner.basicInvalidatePart3(markerEntry, event, true);
    return true;
  }

  public void evictEntry(Object key) {
    // noop
  }

  public void evictValue(Object key) {
    // noop
  }

  /**
   * Used by basicPut to signal the caller that the put was successful.
   */
  private static final RegionEntry markerEntry = new ProxyRegionEntry();

  public RegionEntry basicPut(EntryEventImpl event, long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, boolean overwriteDestroyed)
      throws CacheWriterException, TimeoutException {
    if (!event.isOriginRemote() && event.getOperation() != Operation.REPLACE) { // bug 42167 - don't
                                                                                // convert replace
                                                                                // to CREATE
      event.makeCreate();
    }
    final CacheWriter cacheWriter = this.owner.basicGetWriter();
    final boolean cacheWrite = !event.isOriginRemote() && !event.isNetSearch()
        && !event.getInhibitDistribution() && event.isGenerateCallbacks()
        && (cacheWriter != null || this.owner.hasServerProxy() || this.owner.scope.isDistributed());
    if (cacheWrite) {
      final Set netWriteRecipients;
      if (cacheWriter == null && this.owner.scope.isDistributed()) {
        CacheDistributionAdvisor cda = ((DistributedRegion) this.owner).getDistributionAdvisor();
        netWriteRecipients = cda.adviseNetWrite();
      } else {
        netWriteRecipients = null;
      }
      if (event.getOperation() != Operation.REPLACE) { // bug #42167 - makeCreate() causes REPLACE
                                                       // to eventually become UPDATE
        event.makeCreate();
      }
      this.owner.cacheWriteBeforePut(event, netWriteRecipients, cacheWriter, requireOldValue,
          expectedOldValue);
    }

    owner.recordEvent(event);
    lastModified = // fix for bug 40129
        this.owner.basicPutPart2(event, markerEntry, true, lastModified,
            false /* Clear conflict occurred */);
    this.owner.basicPutPart3(event, markerEntry, true, lastModified, true, ifNew, ifOld,
        expectedOldValue, requireOldValue);
    return markerEntry;
  }

  public void writeSyncIfPresent(Object key, Runnable runner) {
    // nothing needed
  }

  public void removeIfDestroyed(Object key) {
    // nothing needed
  }

  public void txApplyDestroy(Object key, TransactionId txId, TXRmtEvent txEvent,
      boolean inTokenMode, boolean inRI, Operation op, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, boolean isOriginRemote, TXEntryState txEntryState,
      VersionTag versionTag, long tailKey) {
    this.owner.txApplyDestroyPart2(markerEntry, key, inTokenMode,
        false /* Clear conflict occurred */);
    if (!inTokenMode) {
      if (txEvent != null) {
        txEvent.addDestroy(this.owner, markerEntry, key, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldCreateCBEvent(this.owner, !inTokenMode)) {
        // fix for bug 39526
        @Released
        EntryEventImpl e = AbstractRegionMap.createCBEvent(this.owner, op, key, null, txId, txEvent,
            eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag,
            tailKey);
        boolean cbEventInPending = false;
        try {
          AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
          if (pendingCallbacks == null) {
            this.owner.invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, e,
                true/* callDispatchListenerEvent */);
          } else {
            pendingCallbacks.add(e);
            cbEventInPending = true;
          }
        } finally {
          if (!cbEventInPending)
            e.release();
        }
      }
    }
  }

  public void txApplyInvalidate(Object key, Object newValue, boolean didDestroy, TransactionId txId,
      TXRmtEvent txEvent, boolean localOp, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    this.owner.txApplyInvalidatePart2(markerEntry, key, didDestroy, true);
    if (this.owner.isInitialized()) {
      if (txEvent != null) {
        txEvent.addInvalidate(this.owner, markerEntry, key, newValue, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldCreateCBEvent(this.owner, this.owner.isInitialized())) {
        // fix for bug 39526
        boolean cbEventInPending = false;
        @Released
        EntryEventImpl e = AbstractRegionMap.createCBEvent(this.owner,
            localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, key, newValue, txId,
            txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState,
            versionTag, tailKey);
        try {
          AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
          if (pendingCallbacks == null) {
            this.owner.invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, e,
                true/* callDispatchListenerEvent */);
          } else {
            pendingCallbacks.add(e);
            cbEventInPending = true;
          }
        } finally {
          if (!cbEventInPending)
            e.release();
        }
      }
    }
  }

  public void txApplyPut(Operation p_putOp, Object key, Object newValue, boolean didDestroy,
      TransactionId txId, TXRmtEvent txEvent, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    Operation putOp = p_putOp.getCorrespondingCreateOp();
    long lastMod = owner.cacheTimeMillis();
    this.owner.txApplyPutPart2(markerEntry, key, lastMod, true, didDestroy,
        false /* Clear conflict occurred */);
    if (this.owner.isInitialized()) {
      if (txEvent != null) {
        txEvent.addPut(putOp, this.owner, markerEntry, key, newValue, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldCreateCBEvent(this.owner, this.owner.isInitialized())) {
        // fix for bug 39526
        boolean cbEventInPending = false;
        @Released
        EntryEventImpl e = AbstractRegionMap.createCBEvent(this.owner, putOp, key, newValue, txId,
            txEvent, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState,
            versionTag, tailKey);
        try {
          AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
          if (pendingCallbacks == null) {
            this.owner.invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, e,
                true/* callDispatchListenerEvent */);
          } else {
            pendingCallbacks.add(e);
            cbEventInPending = true;
          }
        } finally {
          if (!cbEventInPending)
            e.release();
        }
      }
    }
  }

  // LRUMapCallbacks methods
  public void lruUpdateCallback() {
    // nothing needed
  }

  public boolean disableLruUpdateCallback() {
    // nothing needed
    return false;
  }

  public void enableLruUpdateCallback() {
    // nothing needed
  }

  @Override
  public void decTxRefCount(RegionEntry e) {
    // nothing needed
  }

  @Override
  public boolean lruLimitExceeded(DiskRegionView drv) {
    return false;
  }

  public void lruCloseStats() {
    // nothing needed
  }

  public void resetThreadLocals() {
    // nothing needed
  }

  public void removeEntry(Object key, RegionEntry entry, boolean updateStats) {
    // nothing to do
  }

  public void removeEntry(Object key, RegionEntry re, boolean updateStat, EntryEventImpl event,
      LocalRegion owner) {
    // nothing to do
  }

  /**
   * Provides a dummy implementation of RegionEntry so that basicPut can return an instance that
   * make the upper levels think it did the put.
   */
  public static class ProxyRegionEntry implements RegionEntry {

    public long getLastModified() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public long getLastAccessed() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public long getHitCount() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public long getMissCount() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public VersionStamp getVersionStamp() {
      return null;
    }

    public boolean isTombstone() {
      return false;
    }

    public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
        LocalRegion region, EntryEventImpl event) {
      return null; // proxies don't do versioning
    }

    public void processVersionTag(EntryEvent ev) {
      return;
    }

    public void makeTombstone(LocalRegion r, VersionTag isOperationRemote) {
      return;
    }

    public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
      // do nothing; called by LocalRegion.updateStatsForPut
    }

    public void setRecentlyUsed() {
      // do nothing; called by LocalRegion.updateStatsForPut
    }

    public void updateStatsForGet(boolean hit, long time) {
      // do nothing; no entry stats
    }

    public void txDidDestroy(long currTime) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public void resetCounts() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public void removePhase1(LocalRegion r, boolean isClear) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public void removePhase2() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean isRemoved() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean isRemovedPhase2() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean fillInValue(LocalRegion r, Entry entry, ByteArrayDataInput in, DM mgr,
        final Version version) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean isOverflowedToDisk(LocalRegion r, DistributedRegion.DiskPosition dp) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public Object getKey() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public Object getValue(RegionEntryContext context) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getValueRetain(RegionEntryContext context) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public void setValue(RegionEntryContext context, Object value) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext r, Object val, boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    // @Override
    // public void _setValue(Object value) {
    // throw new
    // UnsupportedOperationException(LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0.toLocalizedString(DataPolicy.EMPTY));
    // }

    @Override
    public Object _getValue() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Token getValueAsToken() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object _getValueRetain(RegionEntryContext context, boolean decompress) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getTransformedValue() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getValueInVM(RegionEntryContext context) {
      return null; // called by TXRmtEvent.createEvent
    }

    public Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public Object getSerializedValueOnDisk(LocalRegion localRegion) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean initialImagePut(LocalRegion region, long lastModified, Object newValue,
        boolean wasRecovered, boolean versionTagAccepted) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean initialImageInit(LocalRegion region, long lastModified, Object newValue,
        boolean create, boolean wasRecovered, boolean versionTagAccepted) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean destroy(LocalRegion region, EntryEventImpl event, boolean inTokenMode,
        boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
        boolean removeRecoveredEntry)
        throws CacheWriterException, EntryNotFoundException, TimeoutException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean getValueWasResultOfSearch() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public void setValueResultOfSearch(boolean v) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    public boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException {
      // note that we don't synchronize on the RE before dispatching
      // events
      event.invokeCallbacks(event.getRegion(), event.inhibitCacheListenerNotification(), false);
      return true;
    }

    public boolean hasStats() {
      return false;
    }

    public Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.geode.internal.cache.RegionEntry#concurrencyCheck(org.apache.geode.internal.cache.
     * LocalRegion, org.apache.geode.internal.cache.versions.VersionTag,
     * org.apache.geode.distributed.internal.membership.InternalDistributedMember,
     * org.apache.geode.distributed.internal.membership.InternalDistributedMember)
     */
    public void processVersionTag(LocalRegion r, VersionTag tag, InternalDistributedMember thisVM,
        InternalDistributedMember sender) {
      return;
    }

    @Override
    public boolean isUpdateInProgress() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void setUpdateInProgress(boolean underUpdate) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isValueNull() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isInvalid() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isDestroyed() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void setValueToNull() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isInvalidOrRemoved() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isDestroyedOrRemoved() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isDestroyedOrRemovedButNotTombstone() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void returnToPool() {
      // TODO Auto-generated method stub

    }

    @Override
    public void setValueWithTombstoneCheck(Object value, EntryEvent event)
        throws RegionClearedException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isCacheListenerInvocationInProgress() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(boolean isListenerInvoked) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
        throws RegionClearedException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isInUseByTransaction() {
      return false;
    }

    @Override
    public void setInUseByTransaction(boolean v) {}

    @Override
    public void incRefCount() {}

    @Override
    public void decRefCount(NewLRUClockHand lruList, LocalRegion lr) {}

    @Override
    public void resetRefCount(NewLRUClockHand lruList) {}

    @Override
    public Object prepareValueForCache(RegionEntryContext r, Object val, EntryEventImpl event,
        boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }
  }

  public void lruUpdateCallback(int n) {
    // do nothing
  }

  public void lruEntryFaultIn(LRUEntry entry) {
    // do nothing.

  }

  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException("copyRecoveredEntries should never be called on proxy");
  }

  public boolean removeTombstone(RegionEntry re, VersionHolder destroyedVersion, boolean isEviction,
      boolean isScheduledTombstone) {
    throw new IllegalStateException("removeTombstone should never be called on a proxy");
  }

  public boolean isTombstoneNotNeeded(RegionEntry re, int destroyedVersion) {
    throw new IllegalStateException("removeTombstone should never be called on a proxy");
  }

  public void setEntryFactory(RegionEntryFactory f) {
    throw new IllegalStateException("Should not be called on a ProxyRegionMap");
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) {
    // Do nothing. Not applicable for clients.
  }

  @Override
  public RegionEntry getEntryInVM(Object key) {
    return null;
  }

  @Override
  public RegionEntry getOperationalEntryInVM(Object key) {
    return null;
  }

  @Override
  public int sizeInVM() {
    return 0;
  }

  @Override
  public void close() {}

  @Override
  public ARMLockTestHook getARMLockTestHook() {
    return null;
  }
}
