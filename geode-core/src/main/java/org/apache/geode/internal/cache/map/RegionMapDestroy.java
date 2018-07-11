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

  private RegionEntry newRegionEntry;
  private RegionEntry regionEntry;
  private RegionEntry tombstoneRegionEntry;

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
    removeRecoveredEntry = removeRecoveredEntryArg || event.isFromRILocalDestroy();
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
      regionEntry = focusedRegionMap.getEntry(event);
      invokeTestHookForConcurrentOperation();
      logDestroy();
      if (regionEntry == null) {
        handleNullRegionEntry();
      } else {
        handleExistingRegionEntry();
      }
    } while (retry);
  }

  private void handleExistingRegionEntry() {
    synchronized (regionEntry) {
      if (tombstoneShouldBeTreatedAsNullEntry()) {
        tombstoneRegionEntry = regionEntry;
        regionEntry = null;
        handleNullRegionEntry();
      } else {
        handleExistingRegionEntryWithIndexInUpdateMode();
      }
    }
  }

  private void afterDestroyActions() {
    try {
      disablePart3IfGatewayConflict();
      triggerDistributionAndListenerNotification();
    } finally {
      if (opCompleted) {
        EntryLogger.logDestroy(event);
        cancelExpiryTaskIfRegionEntryExisted();
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
    retainForConcurrency = false;
    tombstoneRegionEntry = null;
    newRegionEntry = null;
  }

  private void handleNullRegionEntry() {
    retainForConcurrency = isConcurrentNonTombstoneFromRemoteOnReplicaOrFromServer();
    if (inTokenMode || retainForConcurrency) {
      destroyExistingOrAddDestroyedEntryWithIndexInUpdateMode();
    } else {
      finishTombstoneOrEntryNotFound();
    }
  }

  private boolean tombstoneShouldBeTreatedAsNullEntry() {
    // the logic in this class is already very involved, and adding tombstone
    // permutations to (re != null) greatly complicates it. So, we check
    // for a tombstone here and, if found, pretend for a bit that the entry is null
    if (removeRecoveredEntry) {
      return false;
    }
    if (!regionEntry.isTombstone()) {
      return false;
    }
    return true;
  }

  private void logDestroy() {
    if (logger.isTraceEnabled(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE)
        && !(internalRegion instanceof HARegion)) {
      logger.trace(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE,
          "ARM.destroy() inTokenMode={}; duringRI={}; riLocalDestroy={}; withRepl={}; fromServer={}; concurrencyEnabled={}; isOriginRemote={}; isEviction={}; operation={}; re={}",
          inTokenMode, duringRI, event.isFromRILocalDestroy(),
          isReplicate(), event.isFromServer(),
          hasConcurrencyChecks(), event.isOriginRemote(), isEviction,
          event.getOperation(), regionEntry);
    }
  }

  private boolean isConcurrentNonTombstoneFromRemoteOnReplicaOrFromServer() {
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (hasTombstone()) {
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

  private boolean hasConcurrencyChecks() {
    return internalRegion.getConcurrencyChecksEnabled();
  }

  private boolean isReplicaOrFromServer() {
    if (isReplicate()) {
      return true;
    }
    if (event.isFromServer()) {
      return true;
    }
    return false;
  }

  private boolean isReplicate() {
    return internalRegion.getDataPolicy().withReplication();
  }

  private boolean isRemote() {
    if (event.isOriginRemote()) {
      return true;
    }
    if (event.isFromWANAndVersioned()) {
      return true;
    }
    if (event.hasClientOrigin()) {
      return true;
    }
    return false;
  }

  private boolean hasTombstone() {
    return tombstoneRegionEntry != null;
  }

  private void handleExistingRegionEntryWithIndexInUpdateMode() {
    runWithIndexInUpdateMode(this::handleExistingRegionEntryWhileInUpdateMode);
    // No need to call lruUpdateCallback since the only lru action
    // we may have taken was lruEntryDestroy. This fixes bug 31759.
  }

  private void handleExistingRegionEntryWhileInUpdateMode() {
    checkRegionReadiness();
    if (isNotRemovedOrNeedTombstone()) {
      destroyExistingEntry();
    } else {
      handleEntryAlreadyRemoved();
    }
  }

  private void checkRegionReadiness() {
    internalRegion.checkReadiness();
  }

  private void handleEntryAlreadyRemoved() {
    updateVersionTagOnTombstoneEntry();
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

  private boolean destroyShouldContinue() {
    if (regionEntry.isRemovedPhase2()) {
      cleanupRemovedPhase2(regionEntry);
      retry = true;
      return false;
    }
    if (!isEntryReadyForExpiration()) {
      cancelDestroy();
      return false;
    }
    if (!isEntryReadyForEviction(regionEntry)) {
      cancelDestroy();
      return false;
    }
    return true;
  }

  private void throwEntryNotFound() {
    internalRegion.checkEntryNotFound(event.getKey());
  }

  private boolean isOldValueExpected() {
    return expectedOldValue != null;
  }

  private void handleConcurrentModificationException() {
    VersionTag tag = event.getVersionTag();
    if (tag == null) {
      return;
    }
    if (!tag.isTimeStampUpdated()) {
      return;
    }
    // Notify gateways of new time-stamp.
    internalRegion.notifyTimestampsToGateways(event);
  }

  private boolean isNotRemovedOrNeedTombstone() {
    if (!regionEntry.isRemoved()) {
      return true;
    }
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (removeRecoveredEntry) {
      return true;
    }
    if (event.isOriginRemote()) {
      return true;
    }
    if (event.getContext() != null) {
      return true;
    }
    return false;
  }

  private void cancelExpiryTaskIfRegionEntryExisted() {
    if (regionEntry == null) {
      return;
    }
    internalRegion.cancelExpiryTask(regionEntry);
  }

  private void triggerDistributionAndListenerNotification() {
    if (doPart3) {
      // distribution and listener notification
      internalRegion.basicDestroyPart3(regionEntry, event, inTokenMode, duringRI, true,
          expectedOldValue);
    }
  }

  private void disablePart3IfGatewayConflict() {
    if (!event.isConcurrencyConflict()) {
      return;
    }
    if (event.getVersionTag() == null) {
      return;
    }
    if (!event.getVersionTag().isGatewayTag()) {
      return;
    }
    // If concurrency conflict is there and event contains gateway version tag then
    // do NOT distribute.
    doPart3 = false;
  }

  private void updateVersionTagOnTombstoneEntry() {
    if (!regionEntry.isTombstone()) {
      return;
    } // TODO coverage to get here need the following:
    // 1. an existing entry that becomes a tombstone AFTER ifTombstoneSetRegionEntryToNull is called
    // and BEFORE we sync on "regionEntry".
    // 2. event.isOriginRemote to return false
    // 3. removeRecoveredRegion to be false
    // 4. event.getContext to return null
    // 5. concurrencyChecks to be true
    if (event.getVersionTag() == null) {
      return;
    }
    // if we're dealing with a tombstone and this is a remote event
    // (e.g., from cache client update thread) we need to update
    // the tombstone's version information
    // TODO use destroyEntry() here
    processVersionTag(regionEntry);
    try {
      regionEntry.makeTombstone(internalRegion, event.getVersionTag());
    } catch (RegionClearedException e) {
      // that's okay - when writing a tombstone into a disk, the
      // region has been cleared (including this tombstone)
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

  private void cancelDestroy() {
    opCompleted = false;
    retry = false;
  }

  /**
   * @return false if op is an expiration of local origin and the entry is in use by a transaction;
   *         otherwise true
   */
  private boolean isEntryReadyForExpiration() {
    if (!event.getOperation().isExpiration()) {
      return true;
    }
    if (event.isOriginRemote()) {
      return true;
    }
    if (!regionEntry.isInUseByTransaction()) {
      return true;
    }
    return false;
  }

  private void cleanupRemovedPhase2(RegionEntry entry) {
    focusedRegionMap.getEntryMap().remove(event.getKey(), entry);
    internalRegion.getCachePerfStats().incRetries();
  }

  private void finishTombstoneOrEntryNotFound() {
    if (isEviction) {
      return;
    }
    // The following ensures that there is not a concurrent operation
    // on the entry and leaves behind a tombstone if concurrencyChecksEnabled.
    // It fixes bug #32467 by propagating the destroy to the server even though
    // the entry isn't in the client
    if (hasTombstone()) {
      finishTombstone();
    } else {
      finishEntryNotFound();
    }
  }

  private void finishEntryNotFound() {
    newRegionEntry = createNewRegionEntry();
    synchronized (newRegionEntry) {
      if (getExistingOrAddEntry(newRegionEntry) != null) {
        // concurrent change - try again
        retry = true;
        return;
      }
      try {
        handleEntryNotFound(newRegionEntry);
      } finally {
        removeEntryOrLeaveTombstone();
      }
    }
  }

  private void finishTombstone() {
    // Since we sync before testing the region entry to see
    // if it is a tombstone, it is impossible for it to change
    // to something else.
    assert tombstoneRegionEntry.isTombstone();
    // tombstoneRegionEntry came from doing a get on the map.
    // So at this point we know it is still in the map.
    try {
      handleEntryNotFound(tombstoneRegionEntry);
    } finally {
      handleTombstoneVersionTag();
    }
  }

  private void handleTombstoneVersionTag() {
    if (event.getVersionTag() == null) {
      event.setVersionTag(createVersionTagFromStamp(tombstoneRegionEntry.getVersionStamp()));
    } else {
      updateTombstoneVersionTag();
    }
  }

  private RegionEntry createNewRegionEntry() {
    return focusedRegionMap.getEntryFactory().createEntry(internalRegion, event.getKey(),
        Token.REMOVED_PHASE1);
  }

  private void invokeTestHookForConcurrentOperation() {
    /*
     * Execute the test hook runnable inline (not threaded) if it is not null.
     */
    if (null != testHookRunnableForConcurrentOperation) {
      testHookRunnableForConcurrentOperation.run();
    }
  }

  private void destroyExistingEntry() {
    if (!destroyShouldContinue()) {
      return;
    }
    event.setRegionEntry(regionEntry);
    try {
      opCompleted = destroyEntry(regionEntry, false);
      if (opCompleted) {
        handleCompletedDestroyExistingEntry();
      } else {
        handleIncompleteDestroyExistingEntry();
      }
    } catch (RegionClearedException rce) { // TODO coverage
      recordEvent();
      if (inTokenMode && !duringRI) {
        event.inhibitCacheListenerNotification(true);
      }
      handleRegionClearedException(regionEntry);
    } finally {
      if (regionEntry.isRemoved() && !regionEntry.isTombstone()) {
        removeRegionEntryFromMap();
      }
      checkRegionReadiness();
    }
  }

  private void recordEvent() {
    internalRegion.recordEvent(event);
  }

  private void handleCompletedDestroyExistingEntry() {
    // It is very, very important for Partitioned Regions to keep
    // the entry in the map until after distribution occurs so that other
    // threads performing a create on this entry wait until the destroy
    // distribution is finished.
    destroyBeforeRemoval(regionEntry);
    if (!inTokenMode) {
      if (regionEntry.getVersionStamp() == null) {
        regionEntry.removePhase2();
      }
    }
    if (inTokenMode && !duringRI) {
      event.inhibitCacheListenerNotification(true);
    }
    doDestroyPart2(regionEntry, false);
    focusedRegionMap.lruEntryDestroy(regionEntry);
  }

  private void destroyBeforeRemoval(RegionEntry entry) {
    internalRegion.basicDestroyBeforeRemoval(entry, event);
  }

  private void handleIncompleteDestroyExistingEntry() {
    if (inTokenMode) {
      return;
    }
    EntryLogger.logDestroy(event);
    recordEvent();
    if (regionEntry.getVersionStamp() == null) {
      regionEntry.removePhase2();
    } else if (regionEntry.isTombstone() && event.isOriginRemote()) {// TODO coverage
      // the entry is already a tombstone, but we're destroying it
      // again, so we need to reschedule the tombstone's expiration
      internalRegion.rescheduleTombstone(regionEntry, regionEntry.getVersionStamp().asVersionTag());
    }
    focusedRegionMap.lruEntryDestroy(regionEntry);
    opCompleted = true;
  }

  private void removeRegionEntryFromMap() {
    focusedRegionMap.removeEntry(event.getKey(), regionEntry, true, event, internalRegion);
  }

  private void removeEntryOrLeaveTombstone() {
    if (destroyUsingTombstone()) {
      makeNewRegionEntryTombstone();
    } else {
      removeNewEntry();
    }
  }

  private boolean destroyUsingTombstone() {
    if (!hasConcurrencyChecks()) {
      return false;
    }
    if (event.isOriginRemote()) {
      return false;
    }
    if (event.getVersionTag() == null) {
      return false;
    }
    return true;
  }

  private void removeNewEntry() {
    try {
      assert newRegionEntry != tombstoneRegionEntry;
      newRegionEntry.setValue(internalRegion, Token.REMOVED_PHASE2);
      removeNewRegionEntryFromMap();
    } catch (RegionClearedException e) {
      // that's okay - we just need to remove the new entry
    }
  }

  private void removeNewRegionEntryFromMap() {
    focusedRegionMap.removeEntry(event.getKey(), newRegionEntry, false);
  }

  private void updateTombstoneVersionTag() {
    processVersionTag(tombstoneRegionEntry);
    // This code used to call generateAndSetVersionTag if doPart3 was true.
    // But none of the code that calls this method ever sets doPart3 to true.
    assert !doPart3;
    // This is not conflict, we need to persist the tombstone again with new
    // version tag
    try {
      tombstoneRegionEntry.setValue(internalRegion, Token.TOMBSTONE);
    } catch (RegionClearedException e) {
      // that's okay - when writing a tombstone into a disk, the
      // region has been cleared (including this tombstone)
    }
    recordEvent();
    internalRegion.rescheduleTombstone(tombstoneRegionEntry, event.getVersionTag());
    // TODO is it correct that the following code always says "true" for conflictWithClear?
    // Seems like it should only be true if we caught RegionClearedException above.
    doDestroyPart2(tombstoneRegionEntry, true);
    doPart3 = false;
    opCompleted = true;
  }

  private void processVersionTag(RegionEntry entry) {
    try {
      focusedRegionMap.processVersionTag(entry, event);
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    }
  }

  private void makeNewRegionEntryTombstone() {
    processVersionTag(newRegionEntry);
    if (doPart3) {
      internalRegion.generateAndSetVersionTag(event, newRegionEntry);
    }
    try {
      recordEvent();
      newRegionEntry.makeTombstone(internalRegion, event.getVersionTag());
    } catch (RegionClearedException e) {
      // that's okay - when writing a tombstone into a disk, the
      // region has been cleared (including this tombstone)
    }
    opCompleted = true;
    // lruEntryCreate(newRegionEntry);
  }

  private void handleEntryNotFound(RegionEntry entryForDistribution) {
    boolean throwException = false;
    EntryNotFoundException entryNotFoundException = null;

    if (!cacheWrite) {
      throwException = true;
    } else if (!removeRecoveredEntry) {
      try {
        throwException = !internalRegion.bridgeWriteBeforeDestroy(event, expectedOldValue);
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
              event.getKey());
        }
        throwException = false;
        event.setIsRedestroyedEntry(true);
        doPart3 = true;
        // Distribution of this op happens on regionEntry in part 3 so ensure it is not null
        if (regionEntry == null) {
          regionEntry = entryForDistribution;
        }
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
    if (event.isOriginRemote()) {
      return false;
    }
    if (event.getOperation().isLocal()) {
      return false;
    }
    if (event.isFromBridgeAndVersioned()) {
      return true;
    }
    if (event.isFromWANAndVersioned()) {
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
    final IndexManager oqlIndexManager = internalRegion.getIndexManager();
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
    newRegionEntry = createNewRegionEntry();
    synchronized (newRegionEntry) {
      if (handleExistingOrAddNewEntry()) {
        return;
      }
      destroyNewRegionEntry();
    }
  }

  private void destroyNewRegionEntry() {
    regionEntry = newRegionEntry;
    event.setRegionEntry(newRegionEntry);
    if (isEviction) {
      cancelDestroy();
      removeNewRegionEntryFromMap();
      return;
    }
    try {
      // if concurrency checks are enabled, destroy will set the version tag
      destroyEntryAndDoPart2(newRegionEntry);
      // Note no need for LRU work since the entry is destroyed
      // and will be removed when gii completes
    } catch (RegionClearedException rce) {
      handleRegionClearedException(newRegionEntry);
    } finally {
      if (!opCompleted && !hasTombstone()) {
        removeNewRegionEntryFromMap(); // TODO coverage
      }
    }
  }

  /**
   * @return if an entry is already in the map return it; otherwise return null after adding
   *         "toAdd".
   */
  private RegionEntry getExistingOrAddEntry(RegionEntry toAdd) {
    return focusedRegionMap.putEntryIfAbsent(event.getKey(), toAdd);
  }

  /**
   * @return true if existing entry handled; false if new entry added that caller should handle
   */
  private boolean handleExistingOrAddNewEntry() {
    RegionEntry existingRegionEntry = getExistingOrAddEntry(newRegionEntry);
    while (!opCompleted && existingRegionEntry != null) {
      synchronized (existingRegionEntry) {
        if (existingRegionEntry.isRemovedPhase2()) {
          cleanupRemovedPhase2(existingRegionEntry);
          existingRegionEntry = getExistingOrAddEntry(newRegionEntry);
        } else {
          event.setRegionEntry(existingRegionEntry);
          if (!isEntryReadyForEviction(existingRegionEntry)) {
            // An entry existed but could not be destroyed by eviction.
            // So return true to let caller know to do no further work.
            cancelDestroy();
            return true;
          }
          destroyExisting(existingRegionEntry);
          regionEntry = existingRegionEntry;
          opCompleted = true;
        }
      }
    } // while
    return opCompleted;
  }

  private void destroyExisting(RegionEntry existingRegionEntry) {
    boolean conflictWithClear = false;
    try {
      // if concurrency checks are enabled, destroy will set the version tag
      if (!destroyEntry(existingRegionEntry, false)) {
        return; // TODO coverage
      }
      if (retainForConcurrency) {
        // TODO coverage: this will only happen if we find an existing entry after the initial
        // getEntry returned null.
        // So we need putIfAbsent to return an existing entry when getEntry returned null.
        destroyBeforeRemoval(existingRegionEntry);
      }
    } catch (RegionClearedException rce) { // TODO coverage
      conflictWithClear = true;
    }
    doDestroyPart2(existingRegionEntry, conflictWithClear);
    if (!conflictWithClear) {
      focusedRegionMap.lruEntryDestroy(existingRegionEntry);
    }
  }

  private void handleRegionClearedException(RegionEntry entry) {
    // Ignore. The exception will ensure that we do not update the LRU List
    opCompleted = true;
    EntryLogger.logDestroy(event);
    doDestroyPart2(entry, true);
  }

  private void doDestroyPart2(RegionEntry entry, boolean regionCleared) {
    internalRegion.basicDestroyPart2(entry, event, inTokenMode, regionCleared, duringRI, true);
    doPart3 = true;
  }

  private void destroyEntryAndDoPart2(RegionEntry entry) throws RegionClearedException {
    opCompleted = destroyEntry(entry, true);
    if (!opCompleted) {
      return; // TODO coverage
    }
    // This is a new entry that was created because we are in
    // token mode or are accepting a destroy operation by adding
    // a tombstone. There is no oldValue, so we don't need to
    // call updateSizeOnRemove
    event.setIsRedestroyedEntry(true); // native clients need to know if the entry didn't exist
    doDestroyPart2(entry, false);
  }

  private boolean destroyEntry(RegionEntry entry, boolean forceDestroy)
      throws CacheWriterException, TimeoutException,
      EntryNotFoundException, RegionClearedException {
    processVersionTag(entry);
    final int oldSize = internalRegion.calculateRegionEntryValueSize(entry);
    final boolean wasAlreadyRemoved = entry.isDestroyedOrRemoved();
    if (destroyEntryHandleConflict(entry, forceDestroy)) {
      EntryLogger.logDestroy(event);
      if (!wasAlreadyRemoved) {
        internalRegion.updateSizeOnRemove(event.getKey(), oldSize);
      }
      return true;
    }
    return false;
  }

  private boolean destroyEntryHandleConflict(RegionEntry entry, boolean forceDestroy)
      throws CacheWriterException, TimeoutException,
      EntryNotFoundException, RegionClearedException {
    try {
      return entry.destroy(event.getRegion(), event, inTokenMode, cacheWrite,
          expectedOldValue, forceDestroy, removeRecoveredEntry);
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    }
  }

  private VersionTag createVersionTagFromStamp(VersionStamp stamp) {
    VersionTag tag = VersionTag.create(stamp.getMemberID());
    tag.setEntryVersion(stamp.getEntryVersion());
    tag.setRegionVersion(stamp.getRegionVersion());
    tag.setVersionTimeStamp(stamp.getVersionTimeStamp());
    tag.setDistributedSystemId(stamp.getDistributedSystemId());
    return tag;
  }

}
