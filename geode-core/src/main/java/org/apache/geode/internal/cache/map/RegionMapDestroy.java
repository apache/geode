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
package org.apache.geode.internal.cache.map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.sequencelog.EntryLogger;

/**
 * RegionMap Destroy operation.
 *
 * <p>
 * Extracted from AbstractRegionMap.
 */
public class RegionMapDestroy {

  private static final Logger logger = LogService.getLogger();

  static Runnable testHookRunnableForConcurrentOperation;

  private final InternalRegion internalRegion;
  private final FocusedRegionMap focusedRegionMap;
  private final CacheModificationLock cacheModificationLock;

  private final EntryEventImpl event;
  private final boolean inTokenMode;
  private final boolean duringRI;
  private final boolean cacheWrite;
  private final boolean isEviction;
  private final Object expectedOldValue;
  private final boolean removeRecoveredEntry;

  private boolean retry;
  private boolean opCompleted;
  private boolean doPart3;
  private boolean retainForConcurrency;
  private boolean wasTombstone;

  public RegionMapDestroy(InternalRegion internalRegionArg, FocusedRegionMap focusedRegionMapArg,
      CacheModificationLock cacheModificationLockArg, final EntryEventImpl eventArg,
      final boolean inTokenModeArg,
      final boolean duringRIArg, final boolean cacheWriteArg, final boolean isEvictionArg,
      final Object expectedOldValueArg, final boolean removeRecoveredEntryArg) {
    internalRegion = internalRegionArg;
    focusedRegionMap = focusedRegionMapArg;
    cacheModificationLock = cacheModificationLockArg;
    event = eventArg;
    inTokenMode = inTokenModeArg;
    duringRI = duringRIArg;
    cacheWrite = cacheWriteArg;
    isEviction = isEvictionArg;
    expectedOldValue = expectedOldValueArg;
    // for RI local-destroy we don't want to keep tombstones.
    // In order to simplify things we just set this recovery
    // flag to true to force the entry to be removed
    removeRecoveredEntry = removeRecoveredEntryArg || isFromRILocalDestroy();
  }

  public boolean destroy() {
    runWithCacheModificationLock(this::destroyWhileLocked);
    return opCompleted;
  }

  private void destroyWhileLocked() {
    try {
      destroyWithRetry();
    } finally {
      afterDestroyActions();
    }
  }

  private void destroyWithRetry() {
    do {
      initializeState();
      RegionEntry existing = getEntry();
      invokeTestHookForConcurrentOperation();
      logDestroy(existing);
      if (existing == null) {
        handleNullRegionEntry(null);
      } else {
        handleExistingRegionEntry(existing);
      }
    } while (retry);
  }

  private void handleExistingRegionEntry(RegionEntry existing) {
    synchronized (existing) {
      if (tombstoneShouldBeTreatedAsNullEntry(existing)) {
        wasTombstone = true;
        handleNullRegionEntry(existing);
      } else {
        destroyExistingWithIndexInUpdateMode(existing);
      }
    }
  }

  private void afterDestroyActions() {
    RegionEntry destroyedEntry = event.getRegionEntry();
    try {
      disablePart3IfGatewayConflict();
      triggerDistributionAndListenerNotification(destroyedEntry);
    } finally {
      if (opCompleted) {
        EntryLogger.logDestroy(event);
        cancelExpiryTaskIfEntryWasDestroyed(destroyedEntry);
      }
    }
  }

  private void runWithCacheModificationLock(Runnable r) {
    cacheModificationLock.lockForCacheModification(internalRegion, event);
    try {
      r.run();
    } finally {
      cacheModificationLock.releaseCacheModificationLock(internalRegion, event);
    }
  }

  private void initializeState() {
    retry = false;
    opCompleted = false;
    doPart3 = false;
    wasTombstone = false;
    retainForConcurrency = false;
    setRegionEntry(null);
  }

  private void handleNullRegionEntry(RegionEntry tombstone) {
    if (tombstone != null) {
      retainForConcurrency = false;
    } else {
      retainForConcurrency = isConcurrentFromRemoteOnReplicaOrFromServer();
    }
    if (inTokenMode || retainForConcurrency) {
      destroyExistingOrAddDestroyedEntryWithIndexInUpdateMode();
    } else {
      finishTombstoneOrEntryNotFound(tombstone);
    }
  }

  private boolean tombstoneShouldBeTreatedAsNullEntry(RegionEntry entry) {
    // the logic in this class is already very involved, and adding tombstone
    // permutations to (re != null) greatly complicates it. So, we check
    // for a tombstone here and, if found, pretend for a bit that the entry is null
    if (removeRecoveredEntry) {
      return false;
    }
    if (!isTombstone(entry)) {
      return false;
    }
    return true;
  }

  private void logDestroy(RegionEntry entry) {
    if (logger.isTraceEnabled(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE)
        && !(internalRegion instanceof HARegion)) {
      logger.trace(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE,
          "ARM.destroy() inTokenMode={}; duringRI={}; riLocalDestroy={}; withRepl={}; fromServer={}; concurrencyEnabled={}; isOriginRemote={}; isEviction={}; operation={}; re={}",
          inTokenMode, duringRI, isFromRILocalDestroy(),
          isReplicate(), isFromServer(),
          hasConcurrencyChecks(), isOriginRemote(), isEviction,
          getOperation(), entry);
    }
  }

  private boolean isConcurrentFromRemoteOnReplicaOrFromServer() {
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (!isReplicaOrFromServer()) {
      return false;
    }
    if (!isRemote()) {
      return false;
    }
    return true;
  }

  private boolean isReplicaOrFromServer() {
    if (isReplicate()) {
      return true;
    }
    if (isFromServer()) {
      return true;
    }
    return false;
  }

  private boolean isRemote() {
    if (isOriginRemote()) {
      return true;
    }
    if (isFromWANAndVersioned()) {
      return true;
    }
    if (hasClientOrigin()) {
      return true;
    }
    return false;
  }

  private void destroyExistingWithIndexInUpdateMode(RegionEntry existing) {
    runWithIndexInUpdateMode(() -> destroyExistingWhileInUpdateMode(existing));
    // No need to call lruUpdateCallback since the only lru action
    // we may have taken was lruEntryDestroy. This fixes bug 31759.
  }

  private void destroyExistingWhileInUpdateMode(RegionEntry existing) {
    checkRegionReadiness();
    if (isNotRemovedOrNeedTombstone(existing)) {
      destroyExistingFromGetEntry(existing);
    } else {
      handleEntryAlreadyRemoved(existing);
    }
  }

  private void handleEntryAlreadyRemoved(RegionEntry entry) {
    updateVersionTagOnTombstone(entry);
    if (isOldValueExpected()) {
      cancelDestroy();
      return;
    }
    if (inTokenMode) {
      return;
    }
    if (isEviction) {
      return;
    }
    throwEntryNotFound();
  }

  private boolean destroyShouldContinue(RegionEntry entry) {
    if (isRemovedPhase2(entry)) {
      cleanupRemovedPhase2(entry);
      retry = true;
      return false;
    }
    if (!isEntryReadyForExpiration(entry)) {
      cancelDestroy();
      return false;
    }
    if (!isEntryReadyForEviction(entry)) {
      cancelDestroy();
      return false;
    }
    return true;
  }

  private boolean isOldValueExpected() {
    return expectedOldValue != null;
  }

  private void handleConcurrentModificationException() {
    VersionTag tag = getVersionTag();
    if (tag == null) {
      return;
    }
    if (!tag.isTimeStampUpdated()) {
      return;
    }
    notifyGateways();
  }

  private boolean isNotRemovedOrNeedTombstone(RegionEntry entry) {
    if (!isRemoved(entry)) {
      return true;
    }
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (removeRecoveredEntry) {
      return true;
    }
    if (isOriginRemote()) {
      return true;
    }
    if (hasContext()) {
      return true;
    }
    return false;
  }

  private void disablePart3IfGatewayConflict() {
    if (!isConcurrencyConflict()) {
      return;
    }
    if (noVersionTag()) {
      return;
    }
    if (!getVersionTag().isGatewayTag()) {
      return;
    }
    // If concurrency conflict is there and event contains gateway version tag then
    // do NOT distribute.
    doPart3 = false;
  }

  private void updateVersionTagOnTombstone(RegionEntry entry) {
    if (!isTombstone(entry)) {
      return;
    } // TODO coverage to get here need the following:
    // 1. an existing entry that becomes a tombstone AFTER ifTombstoneSetRegionEntryToNull is called
    // and BEFORE we sync on "regionEntry".
    // 2. event.isOriginRemote to return false
    // 3. removeRecoveredRegion to be false
    // 4. event.getContext to return null
    // 5. concurrencyChecks to be true
    if (noVersionTag()) {
      // When will tombstone entries not have a version tag?
      return;
    }
    // if we're dealing with a tombstone and this is a remote event
    // (e.g., from cache client update thread) we need to update
    // the tombstone's version information
    makeTombstone(entry);
  }

  private void cancelDestroy() {
    opCompleted = false;
    retry = false;
  }

  /**
   * @return false if op is an expiration of local origin and the entry is in use by a transaction;
   *         otherwise true
   */
  private boolean isEntryReadyForExpiration(RegionEntry entry) {
    if (!isExpiration()) {
      return true;
    }
    if (isOriginRemote()) {
      return true;
    }
    if (!isInUseByTransaction(entry)) {
      return true;
    }
    return false;
  }

  private void cleanupRemovedPhase2(RegionEntry entry) {
    removeEntry(entry);
    incrementRetryStatistic();
  }

  private void finishTombstoneOrEntryNotFound(RegionEntry tombstone) {
    if (isEviction) {
      return;
    }
    // The following ensures that there is not a concurrent operation
    // on the entry and leaves behind a tombstone if concurrencyChecksEnabled.
    // It fixes bug #32467 by propagating the destroy to the server even though
    // the entry isn't in the client
    if (tombstone != null) {
      finishTombstone(tombstone);
    } else {
      finishEntryNotFound();
    }
  }

  private void finishEntryNotFound() {
    RegionEntry newEntry = createNewRegionEntry();
    synchronized (newEntry) {
      if (getExistingOrAddEntry(newEntry) != null) {
        // concurrent change - try again
        retry = true;
        return;
      }
      try {
        handleEntryNotFound(newEntry);
      } finally {
        makeTombstoneOrRemove(newEntry);
      }
    }
  }

  private void finishTombstone(RegionEntry entry) {
    // Since we sync before testing the region entry to see
    // if it is a tombstone, it is impossible for it to change
    // to something else.
    assert isTombstone(entry);
    // tombstoneRegionEntry came from doing a get on the map.
    // So at this point we know it is still in the map.
    try {
      handleEntryNotFound(entry);
    } finally {
      handleVersionTag(entry);
    }
  }

  private void handleVersionTag(RegionEntry entry) {
    if (noVersionTag()) {
      setVersionTag(entry);
    } else {
      updateVersionTag(entry);
    }
  }

  private void invokeTestHookForConcurrentOperation() {
    if (null != testHookRunnableForConcurrentOperation) {
      testHookRunnableForConcurrentOperation.run();
    }
  }

  private void destroyExistingFromGetEntry(RegionEntry existing) {
    if (!destroyShouldContinue(existing)) {
      return;
    }
    setRegionEntry(existing);
    try {
      opCompleted = destroyEntry(existing, false);
      if (opCompleted) {
        handleCompletedDestroy(existing);
      } else {
        handleIncompleteDestroy(existing);
      }
    } catch (RegionClearedException rce) { // TODO coverage
      recordEvent();
      inhibitCacheListenerNotification();
      handleRegionClearedException(existing);
    } finally {
      if (isRemoved(existing) && !isTombstone(existing)) {
        removeExistingFromMap(existing);
      }
      checkRegionReadiness();
    }
  }

  private void handleCompletedDestroy(RegionEntry existing) {
    // It is very, very important for Partitioned Regions to keep
    // the entry in the map until after distribution occurs so that other
    // threads performing a create on this entry wait until the destroy
    // distribution is finished.
    distributeBucketDestroy(existing);
    if (!inTokenMode) {
      if (!hasVersionStamp(existing)) {
        removePhase2(existing);
      }
    }
    inhibitCacheListenerNotification();
    doDestroyPart2(existing, false);
    lruEntryDestroy(existing);
  }

  private void handleIncompleteDestroy(RegionEntry entry) {
    if (inTokenMode) {
      return;
    }
    recordEvent();
    if (!hasVersionStamp(entry)) {
      removePhase2(entry);
    } else if (isRemoteDestroyOfTombstone(entry)) {
      rescheduleTombstoneUsingEntryTag(entry); // TODO coverage
    }
    lruEntryDestroy(entry);
    opCompleted = true;
  }

  private boolean isRemoteDestroyOfTombstone(RegionEntry entry) {
    if (!isTombstone(entry)) {
      return false;
    }
    if (!isOriginRemote()) {
      return false; // TODO coverage
    }
    return true; // TODO coverage
  }

  private void makeTombstoneOrRemove(RegionEntry entry) {
    if (destroyUsingTombstone()) {
      makeTombstone(entry);
    } else {
      remove(entry);
    }
  }

  private boolean destroyUsingTombstone() {
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (isOriginRemote()) {
      return false;
    }
    if (noVersionTag()) {
      return false;
    }
    return true;
  }

  private void remove(RegionEntry entry) {
    setValue(entry, Token.REMOVED_PHASE2);
    removeFromMap(entry);
  }

  private void updateVersionTag(RegionEntry entry) {
    processVersionTag(entry);
    // This code used to call generateAndSetVersionTag if doPart3 was true.
    // But none of the code that calls this method ever sets doPart3 to true.
    assert !doPart3;
    // This is not conflict, we need to persist the tombstone again with new
    // version tag
    setValue(entry, Token.TOMBSTONE);
    recordEvent();
    rescheduleTombstone(entry);
    // TODO is it correct that the following code always says "true" for conflictWithClear?
    // Seems like it should only be true if we caught RegionClearedException above.
    doDestroyPart2(entry, true);
    doPart3 = false;
    opCompleted = true;
  }

  private void makeTombstone(RegionEntry entry) {
    processVersionTag(entry);
    if (doPart3) {
      generateAndSetVersionTag(entry);
    }
    recordEvent();
    makeTombstoneAndIgnoreClear(entry);
    opCompleted = true;
    // lruEntryCreate(entry);
  }

  private void handleEntryNotFound(RegionEntry entryForDistribution) {
    boolean throwException = false;
    EntryNotFoundException entryNotFoundException = null;

    if (!cacheWrite) {
      throwException = true;
    } else if (!removeRecoveredEntry) {
      try {
        throwException = !bridgeWriteBeforeDestroy();
      } catch (EntryNotFoundException e) {
        throwException = true;
        entryNotFoundException = e;
      }
    }
    if (throwException) {
      if (isVersionedOpFromClientOrWAN()) {
        // we must distribute these since they will update the version information in peers
        if (logger.isDebugEnabled()) {
          logger.debug("ARM.destroy is allowing wan/client destroy of {} to continue",
              getKey());
        }
        throwException = false;
        setRedestroyedEntry();
        doPart3 = true;
        // Distribution of this op happens on regionEntry in part 3 so ensure it is not null
        setRegionEntry(entryForDistribution);
      }
    }
    if (throwException) {
      if (entryNotFoundException != null) {
        throw entryNotFoundException;
      }
      throwEntryNotFound();
    }
  }

  private boolean isVersionedOpFromClientOrWAN() {
    if (isOriginRemote()) {
      return false;
    }
    if (isLocal()) {
      return false;
    }
    if (isFromBridgeAndVersioned()) {
      return true;
    }
    if (isFromWANAndVersioned()) {
      return true;
    }
    return false;
  }

  private void runWithIndexInUpdateMode(Runnable r) {
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
    final IndexManager oqlIndexManager = getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    return oqlIndexManager;
  }

  private void destroyExistingOrAddDestroyedEntryWithIndexInUpdateMode() {
    runWithIndexInUpdateMode(this::destroyExistingOrAddDestroyedEntryWhileInIndexUpdateMode);
  }

  private void destroyExistingOrAddDestroyedEntryWhileInIndexUpdateMode() {
    // removeRecoveredEntry should be false in this case
    RegionEntry newEntry = createNewRegionEntry();
    synchronized (newEntry) {
      if (destroyExistingOrAddNew(newEntry)) {
        return;
      }
      destroyNewEntry(newEntry);
    }
  }

  private void destroyNewEntry(RegionEntry newEntry) {
    setRegionEntry(newEntry);
    if (isEviction) {
      evictDestroy(newEntry);
    } else {
      normalDestroy(newEntry);
    }
  }

  private void evictDestroy(RegionEntry entry) {
    cancelDestroy();
    removeFromMap(entry);
  }

  private void normalDestroy(RegionEntry entry) {
    try {
      // if concurrency checks are enabled, destroy will set the version tag
      destroyEntryAndDoPart2(entry);
      // Note no need for LRU work since the entry is destroyed
      // and will be removed when gii completes
    } catch (RegionClearedException rce) {
      handleRegionClearedException(entry);
    } finally {
      if (opCompleted) {
        return;
      }
      if (wasTombstone) {
        // TODO: why leave the newRegionEntry in the map
        // if we originally had a tombstone?
        // If we get this far with adding newRegionEntry
        // then the original tombstone we saw was gone
        // (because putIfAbsent did not find it in the map
        // if we get here). And since the op did not complete
        // we should then leave newRegionEntry in the map?
        // I think it would be better to just call
        // removeNewRegionEntryFromMap if !opCompleted.
        return;
      }
      removeFromMap(entry);
    }
  }

  /**
   * @return true if existing entry destroyed; false if new entry added that caller should destroy
   */
  private boolean destroyExistingOrAddNew(RegionEntry newEntry) {
    RegionEntry existingEntry = getExistingOrAddEntry(newEntry);
    while (!opCompleted && existingEntry != null) {
      synchronized (existingEntry) {
        if (isRemovedPhase2(existingEntry)) {
          cleanupRemovedPhase2(existingEntry);
          existingEntry = getExistingOrAddEntry(newEntry);
        } else {
          if (!isEntryReadyForEviction(existingEntry)) {
            // An entry existed but could not be destroyed by eviction.
            // So return true to let caller know to do no further work.
            cancelDestroy();
            return true;
          }
          destroyExistingFromPutIfAbsent(existingEntry);
          opCompleted = true;
        }
      }
    } // while
    return opCompleted;
  }

  private void destroyExistingFromPutIfAbsent(RegionEntry existing) {
    boolean conflictWithClear = false;
    setRegionEntry(existing);
    try {
      // if concurrency checks are enabled, destroy will set the version tag
      if (!destroyEntry(existing, false)) {
        // TODO: is this correct? The caller will always set opCompleted to true
        // indicating that this destroy was done even though destroyEntry returned false.
        return;
      }
      if (retainForConcurrency) {
        // TODO this seems like a possible bug that would result in destroys
        // not happening on secondary buckets. It would only happen when an
        // existing entry is returned by putIfAbsent which is very rare.
        // In most cases the initial getEntry call would have found it.
        // But when this does happen on a bucket we need to make sure and
        // distribute that destroy to secondaries. I see no reason why we
        // test "retainForConcurrency".
        // TODO coverage: this will only happen if we find an existing entry after the initial
        // getEntry returned null.
        // So we need putIfAbsent to return an existing entry when getEntry returned null.
        distributeBucketDestroy(existing);
      }
    } catch (RegionClearedException rce) {
      conflictWithClear = true;
    }
    doDestroyPart2(existing, conflictWithClear);
    if (!conflictWithClear) {
      lruEntryDestroy(existing);
    }
  }

  private void handleRegionClearedException(RegionEntry entry) {
    // Ignore. The exception will ensure that we do not update the LRU List
    opCompleted = true;
    doDestroyPart2(entry, true);
  }

  private void destroyEntryAndDoPart2(RegionEntry entry) throws RegionClearedException {
    opCompleted = destroyEntry(entry, true);
    if (!opCompleted) {
      return;
    }
    // This is a new entry that was created because we are in
    // token mode or are accepting a destroy operation by adding
    // a tombstone. There is no oldValue, so we don't need to
    // call updateSizeOnRemove
    setRedestroyedEntry(); // native clients need to know if the entry didn't exist
    doDestroyPart2(entry, false);
  }

  private boolean destroyEntry(RegionEntry entry, boolean forceDestroy)
      throws CacheWriterException, TimeoutException,
      EntryNotFoundException, RegionClearedException {
    processVersionTag(entry);
    final boolean wasAlreadyRemoved = isDestroyedOrRemoved(entry);
    final int oldSize = wasAlreadyRemoved ? 0 : calculateEntryValueSize(entry);
    if (destroyEntryHandleConflict(entry, forceDestroy)) {
      if (!wasAlreadyRemoved) {
        updateSizeOnRemove(oldSize);
      }
      return true;
    }
    return false;
  }

  private VersionTag createVersionTagFromStamp(VersionStamp stamp) {
    VersionTag tag = VersionTag.create(stamp.getMemberID());
    tag.setEntryVersion(stamp.getEntryVersion());
    tag.setRegionVersion(stamp.getRegionVersion());
    tag.setVersionTimeStamp(stamp.getVersionTimeStamp());
    tag.setDistributedSystemId(stamp.getDistributedSystemId());
    return tag;
  }

  // methods that call internalRegion

  private boolean hasConcurrencyChecks() {
    return internalRegion.getConcurrencyChecksEnabled();
  }

  private boolean isReplicate() {
    return internalRegion.getDataPolicy().withReplication();
  }

  private void checkRegionReadiness() {
    internalRegion.checkReadiness();
  }

  private void throwEntryNotFound() {
    internalRegion.checkEntryNotFound(getKey());
  }

  private void notifyGateways() {
    // Notify gateways of new time-stamp.
    internalRegion.notifyTimestampsToGateways(event);
  }

  private void incrementRetryStatistic() {
    internalRegion.getCachePerfStats().incRetries();
  }

  private void recordEvent() {
    internalRegion.recordEvent(event);
  }

  private void distributeBucketDestroy(RegionEntry entry) {
    internalRegion.basicDestroyBeforeRemoval(entry, event);
  }

  private void rescheduleTombstone(RegionEntry entry) {
    internalRegion.rescheduleTombstone(entry, getVersionTag());
  }

  private void rescheduleTombstoneUsingEntryTag(RegionEntry entry) {
    // the entry is already a tombstone, but we're destroying it
    // again, so we need to reschedule the tombstone's expiration
    internalRegion.rescheduleTombstone(entry, getVersionTag(entry));
  }

  private void generateAndSetVersionTag(RegionEntry entry) {
    internalRegion.generateAndSetVersionTag(event, entry);
  }

  private boolean bridgeWriteBeforeDestroy() {
    return internalRegion.bridgeWriteBeforeDestroy(event, expectedOldValue);
  }

  private IndexManager getIndexManager() {
    return internalRegion.getIndexManager();
  }

  private void doDestroyPart2(RegionEntry entry, boolean regionCleared) {
    internalRegion.basicDestroyPart2(entry, event, inTokenMode, regionCleared, duringRI, true);
    doPart3 = true;
  }

  private int calculateEntryValueSize(RegionEntry entry) {
    return internalRegion.calculateRegionEntryValueSize(entry);
  }

  private void updateSizeOnRemove(final int oldSize) {
    internalRegion.updateSizeOnRemove(getKey(), oldSize);
  }

  private void cancelExpiryTaskIfEntryWasDestroyed(RegionEntry entry) {
    if (entry == null) {
      return;
    }
    internalRegion.cancelExpiryTask(entry);
  }

  private void triggerDistributionAndListenerNotification(RegionEntry entry) {
    if (doPart3) {
      // distribution and listener notification
      internalRegion.basicDestroyPart3(entry, event, inTokenMode, duringRI, true,
          expectedOldValue);
    }
  }

  // methods that call event

  private boolean isOriginRemote() {
    return event.isOriginRemote();
  }

  private Object getKey() {
    return event.getKey();
  }

  private Operation getOperation() {
    return event.getOperation();
  }

  private boolean isExpiration() {
    return getOperation().isExpiration();
  }

  private boolean isLocal() {
    return getOperation().isLocal();
  }

  private boolean isFromServer() {
    return event.isFromServer();
  }

  private boolean isFromWANAndVersioned() {
    return event.isFromWANAndVersioned();
  }

  private boolean isFromBridgeAndVersioned() {
    return event.isFromBridgeAndVersioned();
  }

  private boolean isFromRILocalDestroy() {
    return event.isFromRILocalDestroy();
  }

  private boolean isConcurrencyConflict() {
    return event.isConcurrencyConflict();
  }

  private boolean hasClientOrigin() {
    return event.hasClientOrigin();
  }

  private boolean hasContext() {
    return event.getContext() != null;
  }

  private boolean noVersionTag() {
    return getVersionTag() == null;
  }

  private VersionTag getVersionTag() {
    return event.getVersionTag();
  }

  private void setVersionTag(RegionEntry entry) {
    event.setVersionTag(createVersionTagFromStamp(getVersionStamp(entry)));
  }

  private void setRegionEntry(RegionEntry entry) {
    event.setRegionEntry(entry);
  }

  private void setRedestroyedEntry() {
    event.setIsRedestroyedEntry(true);
  }

  private void inhibitCacheListenerNotification() {
    if (inTokenMode && !duringRI) {
      event.inhibitCacheListenerNotification(true);
    }
  }

  // methods that call focusedRegionMap

  private RegionEntry getEntry() {
    return focusedRegionMap.getEntry(event);
  }

  private RegionEntry createNewRegionEntry() {
    return focusedRegionMap.getEntryFactory().createEntry(internalRegion, getKey(),
        Token.REMOVED_PHASE1);
  }

  private boolean removeEntry(RegionEntry entry) {
    return focusedRegionMap.getEntryMap().remove(getKey(), entry);
  }

  private void removeExistingFromMap(RegionEntry entry) {
    focusedRegionMap.removeEntry(getKey(), entry, true, event, internalRegion);
  }

  private void removeFromMap(RegionEntry entry) {
    focusedRegionMap.removeEntry(getKey(), entry, false);
  }

  private void lruEntryDestroy(RegionEntry entry) {
    focusedRegionMap.lruEntryDestroy(entry);
  }

  /**
   * @return if an entry is already in the map return it; otherwise return null after adding
   *         "toAdd".
   */
  private RegionEntry getExistingOrAddEntry(RegionEntry toAdd) {
    return focusedRegionMap.putEntryIfAbsent(getKey(), toAdd);
  }

  private void processVersionTag(RegionEntry entry) {
    try {
      focusedRegionMap.processVersionTag(entry, event);
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    }
  }

  /**
   * @return false if op is an eviction and entry is not ready to be evicted; otherwise true
   */
  private boolean isEntryReadyForEviction(RegionEntry entry) {
    if (!isEviction) {
      return true;
    }
    if (focusedRegionMap.confirmEvictionDestroy(entry)) {
      return true;
    }
    return false;
  }

  // RegionEntry helper methods

  private boolean hasVersionStamp(RegionEntry entry) {
    return getVersionStamp(entry) != null;
  }

  private VersionStamp getVersionStamp(RegionEntry entry) {
    return entry.getVersionStamp();
  }

  private boolean isTombstone(RegionEntry entry) {
    return entry.isTombstone();
  }

  private boolean isRemoved(RegionEntry entry) {
    return entry.isRemoved();
  }

  private boolean isRemovedPhase2(RegionEntry entry) {
    return entry.isRemovedPhase2();
  }

  private boolean isDestroyedOrRemoved(RegionEntry entry) {
    return entry.isDestroyedOrRemoved();
  }

  private boolean isInUseByTransaction(RegionEntry entry) {
    return entry.isInUseByTransaction();
  }

  private VersionTag getVersionTag(RegionEntry entry) {
    return getVersionStamp(entry).asVersionTag();
  }

  private void removePhase2(RegionEntry entry) {
    entry.removePhase2();
  }

  private void makeTombstoneAndIgnoreClear(RegionEntry entry) {
    try {
      entry.makeTombstone(internalRegion, getVersionTag());
    } catch (RegionClearedException e) {
      // that's okay - when writing a tombstone into a disk, the
      // region has been cleared (including this tombstone)
    }
  }

  private void setValue(RegionEntry entry, Object value) {
    try {
      entry.setValue(internalRegion, value);
    } catch (RegionClearedException ignore) {
      // okay to ignore because:
      // 1. when writing a tombstone into a disk, the
      // region has been cleared (including this tombstone).
      // 2. when removing a new entry we just need to remove the new entry.
    }
  }

  private boolean destroyEntryHandleConflict(RegionEntry entry, boolean forceDestroy)
      throws CacheWriterException, TimeoutException,
      EntryNotFoundException, RegionClearedException {
    try {
      return entry.destroy(internalRegion, event, inTokenMode, cacheWrite,
          expectedOldValue, forceDestroy, removeRecoveredEntry);
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    }
  }

}
