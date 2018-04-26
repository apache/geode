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

import java.util.Set;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventSerialization;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.sequencelog.EntryLogger;

public class RegionMapPut {
  private final InternalRegion owner;
  private final FocusedRegionMap focusedRegionMap;
  private final EntryEventImpl event;
  private final boolean ifNew;
  private final boolean ifOld;
  private final boolean overwriteDestroyed;
  private final boolean requireOldValue;
  private final boolean uninitialized;
  private final boolean retrieveOldValueForDelta;
  private final boolean replaceOnClient;
  private final boolean onlyExisting;
  private final boolean cacheWrite;
  private final CacheWriter cacheWriter;
  private final Set netWriteRecipients;
  private final Object expectedOldValue;
  private final CacheModificationLock cacheModificationLock;
  private final EntryEventSerialization entryEventSerialization;
  private boolean clearOccured;
  private long lastModifiedTime;
  private RegionEntry regionEntry;
  private boolean create;
  private boolean completed;
  @Released
  private Object oldValueForDelta;

  public RegionMapPut(FocusedRegionMap focusedRegionMap, InternalRegion owner,
                      CacheModificationLock cacheModificationLock,
                      EntryEventSerialization entryEventSerialization,
                      EntryEventImpl event, boolean ifNew, boolean ifOld,
                      boolean overwriteDestroyed, boolean requireOldValue,
                      Object expectedOldValue) {
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this + " is null for event " + event);
    }
    this.focusedRegionMap = focusedRegionMap;
    this.owner = owner;
    this.cacheModificationLock = cacheModificationLock;
    this.entryEventSerialization = entryEventSerialization;
    this.event = event;
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    this.overwriteDestroyed = overwriteDestroyed;
    this.requireOldValue = requireOldValue;
    this.uninitialized = !owner.isInitialized();
    this.retrieveOldValueForDelta = event.getDeltaBytes() != null && event.getRawNewValue() == null;
    this.replaceOnClient =
        event.getOperation() == Operation.REPLACE && owner.getServerProxy() != null;
    this.onlyExisting = ifOld && !isReplaceOnClient();
    this.cacheWriter = owner.basicGetWriter();
    this.cacheWrite = !event.isOriginRemote() && !event.isNetSearch() && event.isGenerateCallbacks()
        && (getCacheWriter() != null || owner.hasServerProxy() || owner.getScope().isDistributed());
    this.expectedOldValue = expectedOldValue;
    if (isCacheWrite()) {
      if (getCacheWriter() == null && owner.getScope().isDistributed()) {
        this.netWriteRecipients =
            ((DistributedRegion) owner).getCacheDistributionAdvisor().adviseNetWrite();
      } else {
        this.netWriteRecipients = null;
      }
    } else {
      this.netWriteRecipients = null;
    }
  }

  private boolean isIfNew() {
    return ifNew;
  }

  private boolean isIfOld() {
    return ifOld;
  }

  private boolean isOverwriteDestroyed() {
    return overwriteDestroyed;
  }

  private boolean isRequireOldValue() {
    return requireOldValue;
  }

  private boolean isUninitialized() {
    return uninitialized;
  }

  private boolean isRetrieveOldValueForDelta() {
    return retrieveOldValueForDelta;
  }

  private boolean isReplaceOnClient() {
    return replaceOnClient;
  }

  private boolean isOnlyExisting() {
    return onlyExisting;
  }

  private boolean isCacheWrite() {
    return cacheWrite;
  }

  private CacheWriter getCacheWriter() {
    return cacheWriter;
  }

  private Set getNetWriteRecipients() {
    return netWriteRecipients;
  }

  private Object getExpectedOldValue() {
    return expectedOldValue;
  }

  private boolean getClearOccured() {
    return clearOccured;
  }

  private void setClearOccured(boolean clearOccured) {
    this.clearOccured = clearOccured;
  }

  private long getLastModifiedTime() {
    return lastModifiedTime;
  }

  private void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public RegionEntry getRegionEntry() {
    return regionEntry;
  }

  private void setRegionEntry(RegionEntry regionEntry) {
    this.regionEntry = regionEntry;
  }

  /**
   * @return true if put created a new entry; false if it updated an existing one.
   */
  private boolean isCreate() {
    return create;
  }

  private void setCreate(boolean v) {
    this.create = v;
  }

  private EntryEventImpl getEvent() {
    return event;
  }

  public boolean isCompleted() {
    return this.completed;
  }

  private void setCompleted(boolean b) {
    this.completed = b;
  }

  private Object getOldValueForDelta() {
    return this.oldValueForDelta;
  }

  private void setOldValueForDelta(Object value) {
    this.oldValueForDelta = value;
  }

  private InternalRegion getOwner() {
    return owner;
  }

  private FocusedRegionMap getRegionMap() {
    return focusedRegionMap;
  }

  /**
   * @return regionEntry if put completed, otherwise null.
   */
  public RegionEntry put() {
    entryEventSerialization.serializeNewValueIfNeeded(getOwner(), getEvent());
    runWhileLockedForCacheModification(this::doPut);
    if (isCompleted()) {
      return getRegionEntry();
    } else {
      return null;
    }
  }

  private void doPut() {
    try {
      doWithIndexInUpdateMode(this::doPutRetryingIfNeeded);
    } catch (DiskAccessException dae) {
      getOwner().handleDiskAccessException(dae);
      throw dae;
    } finally {
      doAfterPut();
    }
  }

  private void doWithIndexInUpdateMode(Runnable r) {
    final IndexManager oqlIndexManager = getInitializedIndexManager();
    if (oqlIndexManager != null) {
      try {
        r.run();
      } finally {
        oqlIndexManager.countDownIndexUpdaters();
      }
    } else {
      r.run();
    }
  }

  private IndexManager getInitializedIndexManager() {
    IndexManager oqlIndexManager;
    // Fix for Bug #44431. We do NOT want to update the region and wait
    // later for index INIT as region.clear() can cause inconsistency if
    // happened in parallel as it also does index INIT.
    oqlIndexManager = getOwner().getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    return oqlIndexManager;
  }

  private void doPutRetryingIfNeeded() {
    do {
      setRegionEntry(null);
      if (!findExistingEntry()) {
        return;
      }
      createNewEntryIfNeeded();
    } while (!addRegionEntryToMapAndDoPut());
  }

  private void runWhileLockedForCacheModification(Runnable r) {
    cacheModificationLock.lockForCacheModification(getOwner(), getEvent());
    try {
      r.run();
    } finally {
      cacheModificationLock.releaseCacheModificationLock(getOwner(), getEvent());
    }
  }


  /**
   * @return false if an existing entry was not found and this put requires
   *         an existing one; otherwise returns true.
   */
  private boolean findExistingEntry() {
    RegionEntry re = getRegionMap().getEntry(getEvent());
    if (isOnlyExisting()) {
      if (re == null || re.isTombstone()) {
        return false;
      }
    }
    setRegionEntry(re);
    return true;
  }

  /**
   * Stores the created entry in getRegionEntry.
   */
  private void createNewEntryIfNeeded() {
    setCreate(getRegionEntry() == null);
    if (isCreate()) {
      final Object key = getEvent().getKey();
      RegionEntry newEntry = getRegionMap()
          .getEntryFactory().createEntry(getOwner(), key, Token.REMOVED_PHASE1);
      setRegionEntry(newEntry);
    }
  }

  /**
   * @return false if caller should retry
   */
  private boolean addRegionEntryToMapAndDoPut() {
    synchronized (getRegionEntry()) {
      putIfAbsentNewEntry();
      return doPutOnRegionEntry();
    }
  }

  private void putIfAbsentNewEntry() {
    if (isCreate()) {
      RegionEntry oldRe = getRegionMap().putEntryIfAbsent(getEvent().getKey(), getRegionEntry());
      if (oldRe != null) {
        setCreate(false);
        setRegionEntry(oldRe);
      }
    }
  }

  /**
   * @return false if caller should retry
   */
  private boolean doPutOnRegionEntry() {
    final RegionEntry re = getRegionEntry();

    synchronized (re) {
      if (isRegionEntryRemoved()) {
        return false;
      }

      setOldValueForDelta();
      try {
        setOldValueInEvent();
        doCreateOrUpdate();
        return true;
      } finally {
        OffHeapHelper.release(getOldValueForDelta());
        setOldValueForDelta(null);
        if (isCreate() && re.getValueAsToken() == Token.REMOVED_PHASE1) {
          // Region entry remove needs to be done while still synced on re.
          getRegionMap().removeEntry(getEvent().getKey(), re, false);
        }
      }
    }
  }

  private void doAfterPut() {
    if (isCompleted()) {
      try {
        final boolean invokeListeners = getEvent().basicGetNewValue() != Token.TOMBSTONE;
        getOwner().basicPutPart3(getEvent(), getRegionEntry(),
            !isUninitialized(), getLastModifiedTime(), invokeListeners,
            isIfNew(), isIfOld(), getExpectedOldValue(),
            isRequireOldValue());
      } finally {
        if (!getClearOccured()) {
          try {
            getRegionMap().lruUpdateCallback();
          } catch (DiskAccessException dae) {
            getOwner().handleDiskAccessException(dae);
            throw dae;
          }
        }
      }
    } else {
      getRegionMap().resetThreadLocals();
    }
  }

  /**
   * @return false if an early out check indicated that
   *         the put should not be done.
   */
  private boolean shouldPutContinue() {
    if (continueUpdate() && continueOverwriteDestroyed()
        && satisfiesExpectedOldValue()) {
      return true;
    }
    return false;
  }

  private void doCreateOrUpdate() {
    if (!shouldPutContinue()) {
      return;
    }
    invokeCacheWriter();

    runWithIndexUpdatingInProgress(() -> {
      final EntryEventImpl event = getEvent();
      createOrUpdateEntry();
      if (isUninitialized()) {
        event.inhibitCacheListenerNotification(true);
      }
      updateLru();

      final RegionEntry re = getRegionEntry();
      long lastModTime = getOwner().basicPutPart2(event, re, !isUninitialized(),
          getLastModifiedTime(), getClearOccured());
      setLastModifiedTime(lastModTime);
      setCompleted(true);
    });
  }

  private void runWithIndexUpdatingInProgress(Runnable r) {
    notifyIndex(true);
    try {
      r.run();
    } finally {
      notifyIndex(false);
    }
  }

  private void notifyIndex(boolean isUpdating) {
    if (getOwner().getIndexMaintenanceSynchronous()) {
      getRegionEntry().setUpdateInProgress(isUpdating);
    }
  }

  private void createOrUpdateEntry() {
    final EntryEventImpl event = getEvent();
    try {
      if (isUpdate()) {
        updateEntry();
      } else {
        createEntry();
      }
      getOwner().recordEvent(event);
    } catch (RegionClearedException rce) {
      setClearOccured(true);
      getOwner().recordEvent(event);
    } catch (ConcurrentCacheModificationException ccme) {
      VersionTag tag = event.getVersionTag();
      if (tag != null && tag.isTimeStampUpdated()) {
        getOwner().notifyTimestampsToGateways(event);
      }
      throw ccme;
    }
  }

  private boolean isUpdate() {
    if (isCacheWrite() && getEvent().getOperation().isUpdate()) {
      // if there is a cacheWriter, type of event has already been set
      return true;
    }
    if (isReplaceOnClient()) {
      return true;
    }
    if (!getRegionEntry().isRemoved()) {
      return true;
    }
    return false;
  }

  private void setOldValueForDelta() {
    if (isRetrieveOldValueForDelta()) {
      getRegionMap().runWhileEvictionDisabled(() -> {
        // Old value is faulted in from disk if not found in memory.
        setOldValueForDelta(getRegionEntry().getValue(getOwner()));
        // OFFHEAP: if we are synced on region entry no issue since we can use ARE's ref
      });
    }
  }

  /**
   * If the re goes into removed2 state, it will be removed from the map.
   *
   * @return true if re was remove phase 2
   */
  private boolean isRegionEntryRemoved() {
    final RegionEntry re = getRegionEntry();
    if (re.isRemovedPhase2()) {
      getOwner().getCachePerfStats().incRetries();
      getRegionMap().getEntryMap().remove(getEvent().getKey(), re);
      return true;
    } else {
      return false;
    }
  }

  private boolean satisfiesExpectedOldValue() {
    // replace is propagated to server, so no need to check
    // satisfiesOldValue on client
    final EntryEventImpl event = getEvent();
    if (getExpectedOldValue() != null && !isReplaceOnClient()) {
      assert event.getOperation().guaranteesOldValue();
      // We already called setOldValueInEvent so the event will have the old value.
      @Unretained
      Object v = event.getRawOldValue();
      // Note that v will be null instead of INVALID because setOldValue
      // converts INVALID to null.
      // But checkExpectedOldValue handle this and says INVALID equals null.
      if (!AbstractRegionEntry.checkExpectedOldValue(getExpectedOldValue(), v,
          event.getRegion())) {
        return false;
      }
    }
    return true;
  }

  private void setOldValueInEvent() {
    final EntryEventImpl event = getEvent();
    final RegionEntry re = getRegionEntry();
    event.setRegionEntry(re);
    boolean needToSetOldValue = isCacheWrite() || isRequireOldValue()
        || event.getOperation().guaranteesOldValue();
    if (needToSetOldValue) {
      if (event.getOperation().guaranteesOldValue()) {
        // In these cases we want to even get the old value from disk if it is not in memory
        ReferenceCountHelper.skipRefCountTracking();
        @Released
        Object oldValueInVMOrDisk = re.getValueOffHeapOrDiskWithoutFaultIn(event.getRegion());
        ReferenceCountHelper.unskipRefCountTracking();
        try {
          event.setOldValue(oldValueInVMOrDisk, true);
        } finally {
          OffHeapHelper.releaseWithNoTracking(oldValueInVMOrDisk);
        }
      } else {
        // In these cases only need the old value if it is in memory
        ReferenceCountHelper.skipRefCountTracking();

        @Retained
        @Released
        Object oldValueInVM = re.getValueRetain(event.getRegion(), true); // OFFHEAP: re
        // synced so can use
        // its ref.
        if (oldValueInVM == null) {
          oldValueInVM = Token.NOT_AVAILABLE;
        }
        ReferenceCountHelper.unskipRefCountTracking();
        try {
          event.setOldValue(oldValueInVM);
        } finally {
          OffHeapHelper.releaseWithNoTracking(oldValueInVM);
        }
      }
    } else {
      // if the old value is in memory then if it is a GatewaySenderEventImpl then
      // we want to set the old value.
      @Unretained
      Object ov = re.getValue(); // OFFHEAP _getValue is ok since re is synced and we only use it
      // if its a GatewaySenderEventImpl.
      // Since GatewaySenderEventImpl is never stored in an off-heap region nor a compressed region
      // we don't need to worry about ov being compressed.
      if (ov instanceof GatewaySenderEventImpl) {
        event.setOldValue(ov, true);
      }
    }
  }

  private void createEntry() throws RegionClearedException {
    final EntryEventImpl event = getEvent();
    final RegionEntry re = getRegionEntry();
    final boolean wasTombstone = re.isTombstone();
    getRegionMap().processVersionTag(re, event);
    event.putNewEntry(getOwner(), re);
    updateSize(0, false, wasTombstone);
    if (!event.getRegion().isInitialized()) {
      getOwner().getImageState().removeDestroyedEntry(event.getKey());
    }
  }

  private void updateEntry() throws RegionClearedException {
    final EntryEventImpl event = getEvent();
    final RegionEntry re = getRegionEntry();
    final boolean wasTombstone = re.isTombstone();
    final int oldSize = event.getRegion().calculateRegionEntryValueSize(re);
    getRegionMap().processVersionTag(re, event);
    event.putExistingEntry(event.getRegion(), re, isRequireOldValue(),
        getOldValueForDelta());
    EntryLogger.logPut(event);
    updateSize(oldSize, true/* isUpdate */, wasTombstone);
  }

  private void updateLru() {
    if (!getClearOccured()) {
      if (getEvent().getOperation().isCreate()) {
        getRegionMap().lruEntryCreate(getRegionEntry());
      } else {
        getRegionMap().lruEntryUpdate(getRegionEntry());
      }
    }
  }

  private void invokeCacheWriter() {
    final EntryEventImpl event = getEvent();
    // invoke listeners only if region is initialized
    if (getOwner().isInitialized() && isCacheWrite()) {
      // event.setOldValue already called in setOldValueInEvent

      // bug #42638 for replaceOnClient, do not make the event create
      // or update since replace must propagate to server
      if (!isReplaceOnClient()) {
        if (getRegionEntry().isDestroyedOrRemoved()) {
          event.makeCreate();
        } else {
          event.makeUpdate();
        }
      }
      getOwner().cacheWriteBeforePut(event, getNetWriteRecipients(),
          getCacheWriter(), isRequireOldValue(), getExpectedOldValue());
    }
    if (!getOwner().isInitialized() && !isCacheWrite()) {
      // block setting of old value in putNewValueNoSync, don't need it
      event.oldValueNotAvailable();
    }
  }

  private boolean continueOverwriteDestroyed() {
    Token oldValueInVM = getRegionEntry().getValueAsToken();
    // if region is under GII, check if token is destroyed
    if (!isOverwriteDestroyed()) {
      if (!getOwner().isInitialized()
          && (oldValueInVM == Token.DESTROYED || oldValueInVM == Token.TOMBSTONE)) {
        getEvent().setOldValueDestroyedToken();
        return false;
      }
    }
    if (isIfNew() && !Token.isRemoved(oldValueInVM)) {
      return false;
    }
    return true;
  }

  private boolean continueUpdate() {
    if (isIfOld()) {
      final EntryEventImpl event = getEvent();
      final RegionEntry re = getRegionEntry();
      // only update, so just do tombstone maintainence and exit
      if (re.isTombstone() && event.getVersionTag() != null) {
        // refresh the tombstone so it doesn't time out too soon
        getRegionMap().processVersionTag(re, event);
        try {
          re.setValue(getOwner(), Token.TOMBSTONE);
        } catch (RegionClearedException e) {
          // that's okay - when writing a tombstone into a disk, the
          // region has been cleared (including this tombstone)
        }
        getOwner().rescheduleTombstone(re, re.getVersionStamp().asVersionTag());
        return false;
      }
      if (re.isRemoved() && !isReplaceOnClient()) {
        return false;
      }
    }
    return true;
  }

  private void updateSize(int oldSize, boolean isUpdate,
                          boolean wasTombstone) {
    final EntryEventImpl event = getEvent();
    final Object key = event.getKey();
    final int newBucketSize = event.getNewValueBucketSize();
    if (isUpdate && !wasTombstone) {
      getOwner().updateSizeOnPut(key, oldSize, newBucketSize);
    } else {
      getOwner().updateSizeOnCreate(key, newBucketSize);
      if (!wasTombstone) {
        CachePerfStats stats = getOwner().getCachePerfStats();
        if (stats != null) {
          stats.incEntryCount(1);
        }
      }
    }
  }
}
