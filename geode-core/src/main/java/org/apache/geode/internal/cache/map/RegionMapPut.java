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
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.sequencelog.EntryLogger;

public class RegionMapPut extends AbstractRegionMapPut {
  private final CacheModificationLock cacheModificationLock;
  private final EntryEventSerialization entryEventSerialization;
  private final boolean ifNew;
  private final boolean ifOld;
  private final boolean overwriteDestroyed;
  private final boolean requireOldValue;
  private final boolean retrieveOldValueForDelta;
  private final boolean replaceOnClient;
  private final boolean onlyExisting;
  private final boolean cacheWrite;
  private final CacheWriter cacheWriter;
  private final Set netWriteRecipients;
  private final Object expectedOldValue;

  @Released
  private Object oldValueForDelta;

  public RegionMapPut(FocusedRegionMap focusedRegionMap, InternalRegion owner,
      CacheModificationLock cacheModificationLock, EntryEventSerialization entryEventSerialization,
      EntryEventImpl event, boolean ifNew, boolean ifOld, boolean overwriteDestroyed,
      boolean requireOldValue, Object expectedOldValue) {
    super(focusedRegionMap, owner, event);
    this.cacheModificationLock = cacheModificationLock;
    this.entryEventSerialization = entryEventSerialization;
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    this.overwriteDestroyed = overwriteDestroyed;
    this.requireOldValue = requireOldValue;
    this.retrieveOldValueForDelta = event.getDeltaBytes() != null && event.getRawNewValue() == null;
    this.replaceOnClient = event.getOperation() == Operation.REPLACE && owner.hasServerProxy();
    this.onlyExisting = ifOld && !isReplaceOnClient();
    this.cacheWriter = owner.basicGetWriter();
    this.cacheWrite = !event.isOriginRemote() && !event.isNetSearch() && event.isGenerateCallbacks()
        && (getCacheWriter() != null || owner.hasServerProxy() || owner.getScope().isDistributed());
    this.expectedOldValue = expectedOldValue;
    if (isCacheWrite() && getCacheWriter() == null) {
      this.netWriteRecipients = owner.adviseNetWrite();
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

  boolean isRetrieveOldValueForDelta() {
    return retrieveOldValueForDelta;
  }

  boolean isReplaceOnClient() {
    return replaceOnClient;
  }

  boolean isCacheWrite() {
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

  private Object getOldValueForDelta() {
    return this.oldValueForDelta;
  }

  private void setOldValueForDelta(Object value) {
    this.oldValueForDelta = value;
  }

  @Override
  protected boolean isOnlyExisting() {
    return onlyExisting;
  }

  @Override
  protected boolean entryExists(RegionEntry regionEntry) {
    return regionEntry != null && !regionEntry.isTombstone();
  }

  @Override
  protected void serializeNewValueIfNeeded() {
    entryEventSerialization.serializeNewValueIfNeeded(getOwner(), getEvent());
  }

  @Override
  protected void runWhileLockedForCacheModification(Runnable r) {
    cacheModificationLock.lockForCacheModification(getOwner(), getEvent());
    try {
      super.runWhileLockedForCacheModification(r);
    } finally {
      cacheModificationLock.releaseCacheModificationLock(getOwner(), getEvent());
    }
  }

  @Override
  protected void setOldValueForDelta() {
    if (isRetrieveOldValueForDelta()) {
      getRegionMap().runWhileEvictionDisabled(() -> {
        setOldValueForDelta(getRegionEntry().getValue(getOwner()));
      });
    }
  }

  @Override
  protected void setOldValueInEvent() {
    final EntryEventImpl event = getEvent();
    final RegionEntry re = getRegionEntry();
    event.setRegionEntry(re);
    if (event.getOperation().guaranteesOldValue()) {
      setOldValueEvenIfFaultedOut();
    } else if (isCacheWrite() || isRequireOldValue()) {
      setOldValueIfNotFaultedOut();
    } else {
      @Unretained
      Object existingValue = re.getValue();
      if (existingValue instanceof GatewaySenderEventImpl) {
        event.setOldValue(existingValue, true);
      }
    }
  }

  private void setOldValueIfNotFaultedOut() {
    final EntryEventImpl event = getEvent();
    ReferenceCountHelper.skipRefCountTracking();
    @Released
    Object oldValueInVM = getRegionEntry().getValueRetain(event.getRegion(), true);
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

  private void setOldValueEvenIfFaultedOut() {
    final EntryEventImpl event = getEvent();
    ReferenceCountHelper.skipRefCountTracking();
    @Released
    Object oldValueInVMOrDisk =
        getRegionEntry().getValueOffHeapOrDiskWithoutFaultIn(event.getRegion());
    ReferenceCountHelper.unskipRefCountTracking();
    try {
      event.setOldValue(oldValueInVMOrDisk, true);
    } finally {
      OffHeapHelper.releaseWithNoTracking(oldValueInVMOrDisk);
    }
  }

  @Override
  protected void unsetOldValueForDelta() {
    OffHeapHelper.release(getOldValueForDelta());
    setOldValueForDelta(null);
  }

  @Override
  protected void invokeCacheWriter() {
    final EntryEventImpl event = getEvent();
    if (getOwner().isInitialized() && isCacheWrite()) {
      if (!isReplaceOnClient()) {
        if (getRegionEntry().isDestroyedOrRemoved()) {
          event.makeCreate();
        } else {
          event.makeUpdate();
        }
      }
      getOwner().cacheWriteBeforePut(event, getNetWriteRecipients(), getCacheWriter(),
          isRequireOldValue(), getExpectedOldValue());
    }
    if (!getOwner().isInitialized() && !isCacheWrite()) {
      event.oldValueNotAvailable();
    }
  }

  @Override
  protected void createOrUpdateEntry() {
    try {
      if (isUpdate()) {
        updateEntry();
      } else {
        createEntry();
      }
    } catch (RegionClearedException rce) {
      setClearOccurred(true);
    } catch (ConcurrentCacheModificationException ccme) {
      final EntryEventImpl event = getEvent();
      VersionTag tag = event.getVersionTag();
      if (tag != null && tag.isTimeStampUpdated()) {
        getOwner().notifyTimestampsToGateways(event);
      }
      throw ccme;
    }
  }

  @Override
  protected void doBeforeCompletionActions() {
    final EntryEventImpl event = getEvent();
    getOwner().recordEvent(event);
    if (!isOwnerInitialized()) {
      event.inhibitCacheListenerNotification(true);
    }
    updateLru();

    final RegionEntry re = getRegionEntry();
    long lastModTime = getOwner().basicPutPart2(event, re, isOwnerInitialized(),
        getLastModifiedTime(), isClearOccurred());
    setLastModifiedTime(lastModTime);
  }

  private void updateLru() {
    if (!isClearOccurred()) {
      if (getEvent().getOperation().isCreate()) {
        getRegionMap().lruEntryCreate(getRegionEntry());
      } else {
        getRegionMap().lruEntryUpdate(getRegionEntry());
      }
    }
  }

  @Override
  protected boolean shouldCreatedEntryBeRemoved() {
    return getRegionEntry().getValueAsToken() == Token.REMOVED_PHASE1;
  }

  @Override
  protected void doAfterCompletionActions() {
    if (isCompleted()) {
      try {
        final boolean invokeListeners = getEvent().basicGetNewValue() != Token.TOMBSTONE;
        getOwner().basicPutPart3(getEvent(), getRegionEntry(), isOwnerInitialized(),
            getLastModifiedTime(), invokeListeners, isIfNew(), isIfOld(), getExpectedOldValue(),
            isRequireOldValue());
      } finally {
        lruUpdateCallbackIfNotCleared();
      }
    } else {
      getRegionMap().resetThreadLocals();
    }
  }

  private void lruUpdateCallbackIfNotCleared() {
    if (!isClearOccurred()) {
      try {
        getRegionMap().lruUpdateCallback();
      } catch (DiskAccessException dae) {
        getOwner().handleDiskAccessException(dae);
        throw dae;
      }
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

  /**
   * @return false if preconditions indicate that
   *         the put should not be done.
   */
  @Override
  protected boolean checkPreconditions() {
    if (!checkUpdatePreconditions()) {
      return false;
    }
    if (!checkUninitializedRegionPreconditions()) {
      return false;
    }
    if (!checkCreatePreconditions()) {
      return false;
    }
    if (!checkExpectedOldValuePrecondition()) {
      return false;
    }
    return true;
  }

  private boolean checkUpdatePreconditions() {
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

  private boolean checkUninitializedRegionPreconditions() {
    if (!getOwner().isInitialized()) {
      if (!isOverwriteDestroyed()) {
        Token oldValueInVM = getRegionEntry().getValueAsToken();
        if (oldValueInVM == Token.DESTROYED || oldValueInVM == Token.TOMBSTONE) {
          getEvent().setOldValueDestroyedToken();
          return false;
        }
      }
    }
    return true;
  }

  private boolean checkCreatePreconditions() {
    if (isIfNew()) {
      if (!getRegionEntry().isDestroyedOrRemoved()) {
        return false;
      }
    }
    return true;
  }

  private boolean checkExpectedOldValuePrecondition() {
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
      if (!AbstractRegionEntry.checkExpectedOldValue(getExpectedOldValue(), v, event.getRegion())) {
        return false;
      }
    }
    return true;
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
    event.putExistingEntry(event.getRegion(), re, isRequireOldValue(), getOldValueForDelta());
    EntryLogger.logPut(event);
    updateSize(oldSize, true/* isUpdate */, wasTombstone);
  }

  private void updateSize(int oldSize, boolean isUpdate, boolean wasTombstone) {
    final EntryEventImpl event = getEvent();
    final Object key = event.getKey();
    final int newBucketSize = event.getNewValueBucketSize();
    if (isUpdate && !wasTombstone) {
      getOwner().updateSizeOnPut(key, oldSize, newBucketSize);
    } else {
      getOwner().updateSizeOnCreate(key, newBucketSize);
      if (!wasTombstone) {
        getOwner().getCachePerfStats().incEntryCount(1);
      }
    }
  }

}
