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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.AbstractRegionMap.ARMLockTestHook;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;

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

  @Override
  public RegionEntryFactory getEntryFactory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Attributes getAttributes() {
    return this.attr;
  }

  @Override
  public void changeOwner(LocalRegion r) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public Set keySet() {
    return Collections.emptySet();
  }

  @Override
  public Collection<RegionEntry> regionEntries() {
    return Collections.emptySet();
  }

  @Override
  public Collection<RegionEntry> regionEntriesInVM() {
    return Collections.emptySet();
  }

  @Override
  public boolean containsKey(Object key) {
    return false;
  }

  @Override
  public RegionEntry getEntry(Object key) {
    return null;
  }

  @Override
  public RegionEntry putEntryIfAbsent(Object key, RegionEntry re) {
    return null;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Set<VersionSource> clear(RegionVersionVector rvv, BucketRegion bucketRegion) {
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

  @Override
  public RegionEntry initRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Used to modify an existing RegionEntry or create a new one when processing the values obtained
   * during a getInitialImage.
   */
  @Override
  public boolean initialImagePut(Object key, long lastModified, Object newValue,
      boolean wasRecovered, boolean deferLRUCallback, VersionTag entryVersion,
      InternalDistributedMember sender, boolean forceValue) {
    throw new UnsupportedOperationException();
  }

  @Override
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

  @Override
  public boolean invalidate(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry,
      boolean forceCallbacks) throws EntryNotFoundException {

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

  @Override
  public void evictValue(Object key) {
    // noop
  }

  /**
   * Used by basicPut to signal the caller that the put was successful.
   */
  private static final RegionEntry markerEntry = new ProxyRegionEntry();

  @Override
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

  @Override
  public void writeSyncIfPresent(Object key, Runnable runner) {
    // nothing needed
  }

  @Override
  public void removeIfDestroyed(Object key) {
    // nothing needed
  }

  @Override
  public void txApplyDestroy(Object key, TransactionId rmtOrigin, TXRmtEvent event,
      boolean inTokenMode, boolean inRI, Operation op, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, boolean isOperationRemote, TXEntryState txEntryState,
      VersionTag versionTag, long tailKey) {
    this.owner.txApplyDestroyPart2(markerEntry, key, inTokenMode,
        false /* Clear conflict occurred */, false);
    if (!inTokenMode) {
      if (event != null) {
        event.addDestroy(this.owner, markerEntry, key, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldInvokeCallbacks(this.owner, !inTokenMode)) {
        // fix for bug 39526
        @Released
        EntryEventImpl e = AbstractRegionMap.createCallbackEvent(this.owner, op, key, null,
            rmtOrigin, event, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext,
            txEntryState, versionTag, tailKey);
        AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
        pendingCallbacks.add(e);
      }
    }
  }

  @Override
  public void txApplyInvalidate(Object key, Object newValue, boolean didDestroy,
      TransactionId rmtOrigin, TXRmtEvent event, boolean localOp, EventID eventId,
      Object aCallbackArgument, List<EntryEventImpl> pendingCallbacks,
      FilterRoutingInfo filterRoutingInfo, ClientProxyMembershipID bridgeContext,
      TXEntryState txEntryState, VersionTag versionTag, long tailKey) {
    this.owner.txApplyInvalidatePart2(markerEntry, key, didDestroy, true);
    if (this.owner.isInitialized()) {
      if (event != null) {
        event.addInvalidate(this.owner, markerEntry, key, newValue, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldInvokeCallbacks(this.owner, this.owner.isInitialized())) {
        // fix for bug 39526
        @Released
        EntryEventImpl e = AbstractRegionMap.createCallbackEvent(this.owner,
            localOp ? Operation.LOCAL_INVALIDATE : Operation.INVALIDATE, key, newValue, rmtOrigin,
            event, eventId, aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState,
            versionTag, tailKey);
        AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
        pendingCallbacks.add(e);
      }
    }
  }

  @Override
  public void txApplyPut(Operation putOp, Object key, Object newValue, boolean didDestroy,
      TransactionId rmtOrigin, TXRmtEvent event, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    Operation putOperation = putOp.getCorrespondingCreateOp();
    long lastMod = owner.cacheTimeMillis();
    this.owner.txApplyPutPart2(markerEntry, key, lastMod, true, didDestroy, false);
    if (this.owner.isInitialized()) {
      if (event != null) {
        event.addPut(putOperation, this.owner, markerEntry, key, newValue, aCallbackArgument);
      }
      if (AbstractRegionMap.shouldInvokeCallbacks(this.owner, this.owner.isInitialized())) {
        // fix for bug 39526
        @Released
        EntryEventImpl e = AbstractRegionMap.createCallbackEvent(this.owner, putOperation, key,
            newValue, rmtOrigin, event, eventId, aCallbackArgument, filterRoutingInfo,
            bridgeContext, txEntryState, versionTag, tailKey);
        AbstractRegionMap.switchEventOwnerAndOriginRemote(e, txEntryState == null);
        pendingCallbacks.add(e);
      }
    }
  }

  // LRUMapCallbacks methods
  @Override
  public void lruUpdateCallback() {
    // nothing needed
  }

  @Override
  public boolean disableLruUpdateCallback() {
    // nothing needed
    return false;
  }

  @Override
  public void enableLruUpdateCallback() {
    // nothing needed
  }

  @Override
  public void decTxRefCount(RegionEntry e) {
    // nothing needed
  }

  @Override
  public boolean lruLimitExceeded(DiskRegionView diskRegionView) {
    return false;
  }

  @Override
  public void lruCloseStats() {
    // nothing needed
  }

  @Override
  public void resetThreadLocals() {
    // nothing needed
  }

  @Override
  public void removeEntry(Object key, RegionEntry value, boolean updateStats) {
    // nothing to do
  }

  @Override
  public void removeEntry(Object key, RegionEntry re, boolean updateStat, EntryEventImpl event,
      InternalRegion owner) {
    // nothing to do
  }

  /**
   * Provides a dummy implementation of RegionEntry so that basicPut can return an instance that
   * make the upper levels think it did the put.
   */
  public static class ProxyRegionEntry implements RegionEntry {

    @Override
    public long getLastModified() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public long getLastAccessed() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public long getHitCount() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public long getMissCount() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public VersionStamp getVersionStamp() {
      return null;
    }

    @Override
    public boolean isTombstone() {
      return false;
    }

    @Override
    public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
        InternalRegion region, EntryEventImpl event) {
      return null; // proxies don't do versioning
    }

    public void processVersionTag(EntryEvent ev) {
      return;
    }

    @Override
    public void makeTombstone(InternalRegion region, VersionTag version) {
      return;
    }

    @Override
    public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
      // do nothing; called by LocalRegion.updateStatsForPut
    }

    @Override
    public void setRecentlyUsed(RegionEntryContext context) {
      // do nothing; called by LocalRegion.updateStatsForPut
    }

    @Override
    public void updateStatsForGet(boolean hit, long time) {
      // do nothing; no entry stats
    }

    @Override
    public void txDidDestroy(long currentTime) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void resetCounts() throws InternalStatisticsDisabledException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void removePhase1(InternalRegion region, boolean clear) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void removePhase2() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isRemoved() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isRemovedPhase2() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean fillInValue(InternalRegion region, Entry entry, ByteArrayDataInput in,
        DistributionManager distributionManager, final Version version) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean isOverflowedToDisk(InternalRegion region,
        DistributedRegion.DiskPosition diskPosition) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getKey() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
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

    @Override
    public void setValue(RegionEntryContext context, Object value) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext context, Object value,
        boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    @Override
    public Object getValue() {
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
    public Object getValueRetain(RegionEntryContext context, boolean decompress) {
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

    @Override
    public Object getValueOnDisk(InternalRegion region) throws EntryNotFoundException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getValueOnDiskOrBuffer(InternalRegion region) throws EntryNotFoundException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getSerializedValueOnDisk(InternalRegion region) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean initialImagePut(InternalRegion region, long lastModified, Object newValue,
        boolean wasRecovered, boolean acceptedVersionTag) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean initialImageInit(InternalRegion region, long lastModified, Object newValue,
        boolean create, boolean wasRecovered, boolean acceptedVersionTag) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean destroy(InternalRegion region, EntryEventImpl event, boolean inTokenMode,
        boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
        boolean removeRecoveredEntry)
        throws CacheWriterException, EntryNotFoundException, TimeoutException {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean getValueWasResultOfSearch() {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public void setValueResultOfSearch(boolean value) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException {
      // note that we don't synchronize on the RE before dispatching events
      event.invokeCallbacks(event.getRegion(), event.inhibitCacheListenerNotification(), false);
      return true;
    }

    @Override
    public boolean hasStats() {
      return false;
    }

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(InternalRegion region) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(InternalRegion region) {
      throw new UnsupportedOperationException(
          LocalizedStrings.ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0
              .toLocalizedString(DataPolicy.EMPTY));
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
      // nothing
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
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(boolean isListenerInvoked) {
      // nothing
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
    public void incRefCount() {
      // nothing
    }

    @Override
    public void decRefCount(EvictionList lruList, InternalRegion region) {
      // nothing
    }

    @Override
    public void resetRefCount(EvictionList lruList) {
      // nothing
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext context, Object value,
        EntryEventImpl event, boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    @Override
    public boolean isEvicted() {
      return false;
    }
  }

  @Override
  public void lruEntryFaultIn(EvictableEntry entry) {
    // do nothing.

  }

  @Override
  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException("copyRecoveredEntries should never be called on proxy");
  }

  @Override
  public boolean removeTombstone(RegionEntry re, VersionHolder destroyedVersion, boolean isEviction,
      boolean isScheduledTombstone) {
    throw new IllegalStateException("removeTombstone should never be called on a proxy");
  }

  @Override
  public boolean isTombstoneNotNeeded(RegionEntry re, int destroyedVersion) {
    throw new IllegalStateException("removeTombstone should never be called on a proxy");
  }

  @Override
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
  public void close(BucketRegion bucketRegion) {
    // nothing
  }

  @Override
  public ARMLockTestHook getARMLockTestHook() {
    return null;
  }

  @Override
  public long getEvictions() {
    return 0;
  }

  @Override
  public void incRecentlyUsed() {
    // nothing
  }

  @Override
  public EvictionController getEvictionController() {
    return null;
  }

  @Override
  public int getEntryOverhead() {
    return 0;
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

  @Override
  public ConcurrentMapWithReusableEntries<Object, Object> getCustomEntryConcurrentHashMap() {
    return null;
  }

  @Override
  public void setEntryMap(ConcurrentMapWithReusableEntries<Object, Object> map) {

  }
}
