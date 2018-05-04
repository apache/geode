/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.map;

import java.util.List;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TXEntryState;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXRmtEvent;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.sequencelog.EntryLogger;

/**
 * Does a put for a transaction that is being committed.
 */
public class RegionMapCommitPut extends AbstractRegionMapPut {
  private final boolean onlyExisting;
  private final boolean didDestroy;
  private final TXRmtEvent txEvent;
  private final List<EntryEventImpl> pendingCallbacks;
  private final TXEntryState txEntryState;
  private final boolean hasRemoteOrigin;
  private final boolean invokeCallbacks;

  private boolean callbackEventInPending;
  private Operation putOp;
  private boolean oldIsRemoved;

  public RegionMapCommitPut(FocusedRegionMap focusedRegionMap, InternalRegion owner,
      @Released EntryEventImpl callbackEvent, Operation putOp, boolean didDestroy,
      TransactionId txId, TXRmtEvent txEvent, List<EntryEventImpl> pendingCallbacks,
      TXEntryState txEntryState) {
    super(focusedRegionMap, owner, callbackEvent);
    this.putOp = putOp;
    this.didDestroy = didDestroy;
    this.txEvent = txEvent;
    this.pendingCallbacks = pendingCallbacks;
    this.txEntryState = txEntryState;
    this.hasRemoteOrigin = !((TXId) txId).getMemberId().equals(owner.getMyId());
    this.invokeCallbacks = shouldInvokeCallbacks();
    final boolean isTXHost = txEntryState != null;
    final boolean isClientTXOriginator = owner.getCache().isClient() && !hasRemoteOrigin;
    // If we are not a replicate or partitioned (i.e. !isAllEvents)
    // then only apply the update to existing entries.
    // If we are a replicate or partitioned then only apply the update to
    // existing entries when the operation is an update and we
    // are initialized.
    // Otherwise use the standard create/update logic.
    this.onlyExisting = hasRemoteOrigin && !isTXHost && !isClientTXOriginator
        && (!owner.isAllEvents() || (!putOp.isCreate() && isOwnerInitialized()));
  }

  private Operation getPutOp() {
    return this.putOp;
  }

  private boolean shouldInvokeCallbacks() {
    InternalRegion owner = getOwner();
    boolean isPartitioned = owner.isUsedForPartitionedRegionBucket();
    if (isPartitioned) {
      owner = owner.getPartitionedRegion();
    }
    return (isPartitioned || isOwnerInitialized()) && (owner.shouldDispatchListenerEvent()
        || owner.shouldNotifyBridgeClients() || owner.getConcurrencyChecksEnabled());
  }

  private void setCallbackEventInPending(boolean v) {
    this.callbackEventInPending = v;
  }

  private boolean isCallbackEventInPending() {
    return this.callbackEventInPending;
  }

  private void makeCreate() {
    putOp = putOp.getCorrespondingCreateOp();
  }

  private void makeUpdate() {
    putOp = putOp.getCorrespondingUpdateOp();
  }

  @Override
  protected boolean isOnlyExisting() {
    return this.onlyExisting;
  }

  @Override
  protected boolean entryExists(RegionEntry regionEntry) {
    return regionEntry != null && !regionEntry.isRemoved();
  }

  @Override
  protected void serializeNewValueIfNeeded() {
    // nothing needed
  }

  @Override
  protected void runWhileLockedForCacheModification(Runnable r) {
    // commit has already done the locking
    r.run();
  }

  @Override
  protected void setOldValueForDelta() {
    // nothing needed
  }

  @Override
  protected void setOldValueInEvent() {
    if (!isCreate()) {
      Object oldValue = getRegionEntry().getValueInVM(getOwner());
      getEvent().setOldValue(oldValue);
    }
  }

  @Override
  protected void unsetOldValueForDelta() {
    // nothing needed
  }

  @Override
  protected boolean checkPreconditions() {
    // nothing needed
    return true;
  }

  @Override
  protected void invokeCacheWriter() {
    // nothing needed
  }

  @Override
  protected void createOrUpdateEntry() {
    final RegionEntry regionEntry = getRegionEntry();
    final EntryEventImpl callbackEvent = getEvent();
    final InternalRegion owner = getOwner();
    final FocusedRegionMap regionMap = getRegionMap();
    final boolean isCreate = isCreate();
    final Object key = callbackEvent.getKey();
    final Object newValue = callbackEvent.getRawNewValueAsHeapObject();

    if (isCreate) {
      makeCreate();
    } else {
      if (isOnlyExisting() && regionEntry.isRemoved()) {
        return;
      }
      setCompleted(true);
      if (!regionEntry.isRemoved()) {
        makeUpdate();
      }
    }
    final int oldSize = isCreate ? 0 : getOwner().calculateRegionEntryValueSize(regionEntry);
    this.oldIsRemoved = isCreate ? true : regionEntry.isDestroyedOrRemoved();
    callbackEvent.setRegionEntry(regionEntry);
    regionMap.txRemoveOldIndexEntry(getPutOp(), regionEntry);
    setLastModifiedTime(owner.cacheTimeMillis());
    if (didDestroy) {
      regionEntry.txDidDestroy(getLastModifiedTime());
    }
    if (txEvent != null) {
      txEvent.addPut(getPutOp(), owner, regionEntry, key, newValue,
          callbackEvent.getCallbackArgument());
    }
    regionEntry.setValueResultOfSearch(getPutOp().isNetSearch());
    try {
      regionMap.processAndGenerateTXVersionTag(callbackEvent, regionEntry, txEntryState);
      setNewValueOnRegionEntry(newValue);
      if (getPutOp().isCreate()) {
        owner.updateSizeOnCreate(key, owner.calculateRegionEntryValueSize(regionEntry));
      } else if (getPutOp().isUpdate()) {
        owner.updateSizeOnPut(key, oldSize, owner.calculateRegionEntryValueSize(regionEntry));
      }
    } catch (RegionClearedException rce) {
      setClearOccurred(true);
    }
    EntryLogger.logTXPut(owner, key, newValue);
  }

  private void setNewValueOnRegionEntry(final Object newValue) throws RegionClearedException {
    final RegionEntry regionEntry = getRegionEntry();
    final InternalRegion owner = getOwner();
    final boolean wasTombstone = regionEntry.isTombstone();
    final Object preparedValue =
        regionEntry.prepareValueForCache(owner, newValue, getEvent(), !getPutOp().isCreate());
    regionEntry.setValue(owner, preparedValue);
    if (wasTombstone) {
      owner.unscheduleTombstone(regionEntry);
    }
  }

  @Override
  protected boolean shouldCreatedEntryBeRemoved() {
    return !this.isCompleted();
  }

  @Override
  protected void doBeforeCompletionActions() {
    final RegionEntry regionEntry = getRegionEntry();
    final EntryEventImpl callbackEvent = getEvent();
    final InternalRegion owner = getOwner();
    final FocusedRegionMap regionMap = getRegionMap();
    final boolean isCreate = isCreate();
    final Object key = callbackEvent.getKey();

    regionEntry.updateStatsForPut(getLastModifiedTime(), getLastModifiedTime());
    owner.txApplyPutPart2(regionEntry, key, getLastModifiedTime(), isCreate, didDestroy,
        isClearOccurred());
    if (isCreate) {
      setCompleted(true);
    }
    if (invokeCallbacks) {
      if (isCreate) {
        callbackEvent.makeCreate();
        callbackEvent.setOldValue(null);
      } else {
        if (!oldIsRemoved) {
          callbackEvent.makeUpdate();
        }
      }
      callbackEvent.changeRegionToBucketsOwner();
      callbackEvent.setOriginRemote(hasRemoteOrigin);
      pendingCallbacks.add(callbackEvent);
      setCallbackEventInPending(true);
    }
    if (!isClearOccurred()) {
      if (isCreate) {
        regionMap.lruEntryCreate(regionEntry);
        regionMap.incEntryCount(1);
      } else {
        regionMap.lruEntryUpdate(regionEntry);
      }
    }
  }

  @Override
  protected void doAfterCompletionActions() {
    if (isOnlyExisting() && !isCompleted()) {
      if (didDestroy) {
        getOwner().txApplyPutHandleDidDestroy(getEvent().getKey());
      }
      if (invokeCallbacks) {
        getEvent().makeUpdate();
        getOwner().invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, getEvent(), false);
      }
    }
    if (getOwner().getConcurrencyChecksEnabled() && txEntryState != null) {
      txEntryState.setVersionTag(getEvent().getVersionTag());
    }
    if (!isCallbackEventInPending()) {
      getEvent().release();
    }
  }

}
