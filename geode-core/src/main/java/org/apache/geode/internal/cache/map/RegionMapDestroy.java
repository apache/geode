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
import org.apache.geode.internal.Assert;
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
import org.apache.geode.internal.offheap.annotations.Released;
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

  private boolean retry = true;
  private boolean opCompleted = false;
  private boolean doPart3 = false;
  private boolean retainForConcurrency = false;

  private boolean abortDestroyAndReturnFalse;
  private RegionEntry oldRegionEntry;
  private RegionEntry newRegionEntry;
  private RegionEntry regionEntry;
  private RegionEntry tombstone;
  private boolean haveTombstone;
  private boolean doContinue;

  public RegionMapDestroy(InternalRegion internalRegion, FocusedRegionMap focusedRegionMap,
      CacheModificationLock cacheModificationLock, final EntryEventImpl eventArg,
      final boolean inTokenModeArg,
      final boolean duringRIArg, final boolean cacheWriteArg, final boolean isEvictionArg,
      final Object expectedOldValueArg, final boolean removeRecoveredEntryArg) {
    this.internalRegion = internalRegion;
    this.focusedRegionMap = focusedRegionMap;
    this.cacheModificationLock = cacheModificationLock;
    this.event = eventArg;
    this.inTokenMode = inTokenModeArg;
    this.duringRI = duringRIArg;
    this.cacheWrite = cacheWriteArg;
    this.isEviction = isEvictionArg;
    this.expectedOldValue = expectedOldValueArg;
    // for RI local-destroy we don't want to keep tombstones.
    // In order to simplify things we just set this recovery
    // flag to true to force the entry to be removed
    this.removeRecoveredEntry = removeRecoveredEntryArg || event.isFromRILocalDestroy();
  }

  public boolean destroy()
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    cacheModificationLock.lockForCacheModification(internalRegion, event);
    try {
      while (retry) {
        initializeRetryLoop();
        regionEntry = focusedRegionMap.getEntry(event);
        invokeTestHookForConcurrentOperation();
        try {
          logDestroy();
          ifTombstoneSetRegionEntryToNull();
          if (regionEntry == null) {
            handleNullRegionEntry();
          } else {
            handleExistingRegionEntry();
          }
          if (abortDestroyAndReturnFalse) {
            return false;
          }
          if (doContinue) {
            continue;
          }
          if (opCompleted) {
            EntryLogger.logDestroy(event);
          }
          return opCompleted;
        } finally {
          try {
            disablePart3IfGatewayConflict();
            triggerDistributionAndListenerNotification();
          } finally {
            cancelExpiryTaskIfRegionEntryExisted();
          }
        }
      } // retry loop
    } finally {
      cacheModificationLock.releaseCacheModificationLock(internalRegion, event);
    }
    return false;
  }

  private void initializeRetryLoop() {
    retry = false;
    opCompleted = false;
    tombstone = null;
    haveTombstone = false;
    doContinue = false;
    abortDestroyAndReturnFalse = false;
  }

  private void handleNullRegionEntry() {
    calculateRetainForConcurrency();
    if (inTokenMode || retainForConcurrency) {
      handleMissingRegionEntry();
    } else {
      retryRemoveWithTombstone();
    }
  }

  private void ifTombstoneSetRegionEntryToNull() {
    // the logic in this method is already very involved, and adding tombstone
    // permutations to (re != null) greatly complicates it. So, we check
    // for a tombstone here and, if found, pretend for a bit that the entry is null
    if (regionEntry != null && regionEntry.isTombstone() && !removeRecoveredEntry) {
      tombstone = regionEntry;
      haveTombstone = true;
      regionEntry = null;
    }
  }

  private void logDestroy() {
    if (logger.isTraceEnabled(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE)
        && !(internalRegion instanceof HARegion)) {
      logger.trace(LogMarker.LRU_TOMBSTONE_COUNT_VERBOSE,
          "ARM.destroy() inTokenMode={}; duringRI={}; riLocalDestroy={}; withRepl={}; fromServer={}; concurrencyEnabled={}; isOriginRemote={}; isEviction={}; operation={}; re={}",
          inTokenMode, duringRI, event.isFromRILocalDestroy(),
          internalRegion.getDataPolicy().withReplication(), event.isFromServer(),
          internalRegion.getConcurrencyChecksEnabled(), event.isOriginRemote(), isEviction,
          event.getOperation(), regionEntry);
    }
  }

  private void calculateRetainForConcurrency() {
    // we need to create an entry if in token mode or if we've received
    // a destroy from a peer or WAN gateway and we need to retain version
    // information for concurrency checks
    retainForConcurrency = (!haveTombstone
        && (internalRegion.getDataPolicy().withReplication()
            || event.isFromServer())
        && internalRegion.getConcurrencyChecksEnabled()
        && (event.isOriginRemote() // remote event must create a tombstone
            || event.isFromWANAndVersioned() // wan event must create a tombstone
            || event.isBridgeEvent())); // client event must create a tombstone
  }

  private void handleExistingRegionEntry() {
    IndexManager oqlIndexManager = internalRegion.getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    try {
      synchronized (regionEntry) {
        internalRegion.checkReadiness();
        if (!regionEntry.isRemoved() || createTombstoneForConflictChecks()) {
          if (isRemovedPhase2()) {
            return;
          }
          if (isEntryIsInUseByTransaction()) {
            return;
          }
          event.setRegionEntry(regionEntry);
          if (isEntryReadyForEviction(regionEntry)) {
            return;
          }
          destroyExistingEntry();
        } else { // already removed
          updateVersionTagOnEntryWithTombstone();
          if (isOldValueExpected()) {
            return;
          }
          if (!inTokenMode && !isEviction) {
            throwEntryNotFound();
          }
        }
      } // synchronized re
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    } finally {
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
    }
    // No need to call lruUpdateCallback since the only lru action
    // we may have taken was lruEntryDestroy. This fixes bug 31759.
  }

  private void throwEntryNotFound() {
    internalRegion.checkEntryNotFound(event.getKey());
  }

  private boolean isOldValueExpected() {
    if (expectedOldValue != null) {
      abortDestroyAndReturnFalse = true;
      return true;
    }
    return false;
  }

  private void handleConcurrentModificationException() {
    VersionTag tag = event.getVersionTag();
    if (tag != null && tag.isTimeStampUpdated()) {
      // Notify gateways of new time-stamp.
      internalRegion.notifyTimestampsToGateways(event);
    }
  }

  private boolean createTombstoneForConflictChecks() {
    // if the entry is a tombstone and the event is from a peer or a client
    // then we allow the operation to be performed so that we can update the
    // version stamp. Otherwise we would retain an old version stamp and may allow
    // an operation that is older than the destroy() to be applied to the cache
    // Bug 45170: If removeRecoveredEntry, we treat tombstone as regular entry to be
    // deleted
    return internalRegion.getConcurrencyChecksEnabled()
        && (event.isOriginRemote() || event.getContext() != null || removeRecoveredEntry);
  }

  private void cancelExpiryTaskIfRegionEntryExisted() {
    if (opCompleted) {
      if (regionEntry != null) {
        // we only want to cancel if concurrency-check is not enabled
        // regionEntry will be null when concurrency-check is enable and removeTombstone
        // method
        // will call cancelExpiryTask on regionEntry
        internalRegion.cancelExpiryTask(regionEntry);
      }
    }
  }

  private void triggerDistributionAndListenerNotification() {
    // distribution and listener notification
    if (doPart3) {
      internalRegion.basicDestroyPart3(regionEntry, event, inTokenMode, duringRI, true,
          expectedOldValue);
    }
  }

  private void disablePart3IfGatewayConflict() {
    // If concurrency conflict is there and event contains gateway version tag then
    // do NOT distribute.
    if (event.isConcurrencyConflict()
        && (event.getVersionTag() != null && event.getVersionTag().isGatewayTag())) {
      doPart3 = false;
    }
  }

  private void updateVersionTagOnEntryWithTombstone() {
    if (regionEntry.isTombstone() && event.getVersionTag() != null) {
      // if we're dealing with a tombstone and this is a remote event
      // (e.g., from cache client update thread) we need to update
      // the tombstone's version information
      // TODO use destroyEntry() here
      focusedRegionMap.processVersionTag(regionEntry, event);
      try {
        regionEntry.makeTombstone(internalRegion, event.getVersionTag());
      } catch (RegionClearedException e) {
        // that's okay - when writing a tombstone into a disk, the
        // region has been cleared (including this tombstone)
      }
    }
  }

  private boolean isEntryReadyForEviction(RegionEntry entry) {
    if (isEviction) {
      if (!focusedRegionMap.confirmEvictionDestroy(entry)) {
        opCompleted = false;
        abortDestroyAndReturnFalse = true;
        return true;
      }
    }
    return false;
  }

  private boolean isEntryIsInUseByTransaction() {
    if (!event.isOriginRemote() && event.getOperation().isExpiration()) {
      // If this expiration started locally then only do it if the RE is not being
      // used by a tx.
      if (regionEntry.isInUseByTransaction()) {
        opCompleted = false;
        abortDestroyAndReturnFalse = true;
        return true;
      }
    }
    return false;
  }

  private boolean isRemovedPhase2() {
    if (regionEntry.isRemovedPhase2()) {
      focusedRegionMap.getEntryMap().remove(event.getKey(), regionEntry);
      internalRegion.getCachePerfStats().incRetries();
      retry = true;
      doContinue = true;
      return true;
    }
    return false;
  }

  private void retryRemoveWithTombstone() {
    if (!isEviction || internalRegion.getConcurrencyChecksEnabled()) {
      // The following ensures that there is not a concurrent operation
      // on the entry and leaves behind a tombstone if concurrencyChecksEnabled.
      // It fixes bug #32467 by propagating the destroy to the server even though
      // the entry isn't in the client
      newRegionEntry = haveTombstone ? tombstone
          : focusedRegionMap.getEntryFactory().createEntry(internalRegion, event.getKey(),
              Token.REMOVED_PHASE1);
      synchronized (newRegionEntry) {
        if (haveTombstone && !tombstone.isTombstone()) {
          // we have to check this again under synchronization since it may have changed
          retry = true;
          doContinue = true;
          return;
        }
        regionEntry = (RegionEntry) focusedRegionMap.getEntryMap().putIfAbsent(event.getKey(),
            newRegionEntry);
        if (regionEntry != null && regionEntry != tombstone) {
          // concurrent change - try again
          retry = true;
          doContinue = true;
          return;
        } else if (!isEviction) {
          try {
            handleEntryNotFound();
          } finally {
            removeEntryOrLeaveTombstone();
          }
        }
      } // synchronized(newRegionEntry)
    }
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
    boolean removed = false;
    try {
      opCompleted = destroyEntry(regionEntry, event, inTokenMode, cacheWrite, expectedOldValue,
          false, removeRecoveredEntry);
      if (opCompleted) {
        // It is very, very important for Partitioned Regions to keep
        // the entry in the map until after distribution occurs so that other
        // threads performing a create on this entry wait until the destroy
        // distribution is finished.
        // keeping backup copies consistent. Fix for bug 35906.
        internalRegion.basicDestroyBeforeRemoval(regionEntry, event);

        // do this before basicDestroyPart2 to fix bug 31786
        if (!inTokenMode) {
          if (regionEntry.getVersionStamp() == null) {
            regionEntry.removePhase2();
            focusedRegionMap.removeEntry(event.getKey(), regionEntry, true, event, internalRegion);
            removed = true;
          }
        }
        if (inTokenMode && !duringRI) {
          event.inhibitCacheListenerNotification(true);
        }
        doPart3 = true;
        internalRegion.basicDestroyPart2(regionEntry, event, inTokenMode,
            false /* conflict with clear */, duringRI, true);
        focusedRegionMap.lruEntryDestroy(regionEntry);
      } else {
        if (!inTokenMode) {
          EntryLogger.logDestroy(event);
          internalRegion.recordEvent(event);
          if (regionEntry.getVersionStamp() == null) {
            regionEntry.removePhase2();
            focusedRegionMap.removeEntry(event.getKey(), regionEntry, true, event, internalRegion);
            focusedRegionMap.lruEntryDestroy(regionEntry);
          } else {
            if (regionEntry.isTombstone()) {
              // the entry is already a tombstone, but we're destroying it
              // again, so we need to reschedule the tombstone's expiration
              if (event.isOriginRemote()) {
                internalRegion.rescheduleTombstone(regionEntry,
                    regionEntry.getVersionStamp().asVersionTag());
              }
            }
          }
          focusedRegionMap.lruEntryDestroy(regionEntry);
          opCompleted = true;
        }
      }
    } catch (RegionClearedException rce) {
      // Ignore. The exception will ensure that we do not update
      // the LRU List
      opCompleted = true;
      internalRegion.recordEvent(event);
      if (inTokenMode && !duringRI) {
        event.inhibitCacheListenerNotification(true);
      }
      internalRegion.basicDestroyPart2(regionEntry, event, inTokenMode,
          true /* conflict with clear */, duringRI, true);
      doPart3 = true;
    } finally {
      internalRegion.checkReadiness();
      if (regionEntry.isRemoved() && !regionEntry.isTombstone()) {
        if (!removed) {
          focusedRegionMap.removeEntry(event.getKey(), regionEntry, true, event, internalRegion);
        }
      }
    }
  }

  private void removeEntryOrLeaveTombstone() {
    // either remove the entry or leave a tombstone
    try {
      if (!event.isOriginRemote() && event.getVersionTag() != null
          && internalRegion.getConcurrencyChecksEnabled()) {
        // this shouldn't fail since we just created the entry.
        // it will either generate a tag or apply a server's version tag
        focusedRegionMap.processVersionTag(newRegionEntry, event);
        if (doPart3) {
          internalRegion.generateAndSetVersionTag(event, newRegionEntry);
        }
        try {
          internalRegion.recordEvent(event);
          newRegionEntry.makeTombstone(internalRegion, event.getVersionTag());
        } catch (RegionClearedException e) {
          // that's okay - when writing a tombstone into a disk, the
          // region has been cleared (including this tombstone)
        }
        opCompleted = true;
        // lruEntryCreate(newRegionEntry);
      } else if (!haveTombstone) {
        try {
          assert newRegionEntry != tombstone;
          newRegionEntry.setValue(internalRegion, Token.REMOVED_PHASE2);
          focusedRegionMap.removeEntry(event.getKey(), newRegionEntry, false);
        } catch (RegionClearedException e) {
          // that's okay - we just need to remove the new entry
        }
      } else if (event.getVersionTag() != null) { // haveTombstone - update the
        // tombstone version info
        focusedRegionMap.processVersionTag(tombstone, event);
        if (doPart3) {
          internalRegion.generateAndSetVersionTag(event, newRegionEntry);
        }
        // This is not conflict, we need to persist the tombstone again with new
        // version tag
        try {
          tombstone.setValue(internalRegion, Token.TOMBSTONE);
        } catch (RegionClearedException e) {
          // that's okay - when writing a tombstone into a disk, the
          // region has been cleared (including this tombstone)
        }
        internalRegion.recordEvent(event);
        internalRegion.rescheduleTombstone(tombstone, event.getVersionTag());
        internalRegion.basicDestroyPart2(tombstone, event, inTokenMode,
            true /* conflict with clear */, duringRI, true);
        opCompleted = true;
      } else {
        Assert.assertTrue(event.getVersionTag() == null);
        Assert.assertTrue(newRegionEntry == tombstone);
        event.setVersionTag(getVersionTagFromStamp(tombstone.getVersionStamp()));
      }
    } catch (ConcurrentCacheModificationException e) {
      handleConcurrentModificationException();
      throw e;
    }
  }

  private void handleEntryNotFound() {
    boolean throwException = false;
    EntryNotFoundException entryNotFoundException = null;

    if (!cacheWrite) {
      throwException = true;
    } else {
      try {
        if (!removeRecoveredEntry) {
          throwException = !internalRegion.bridgeWriteBeforeDestroy(event, expectedOldValue);
        }
      } catch (EntryNotFoundException e) {
        throwException = true;
        entryNotFoundException = e;
      }
    }
    if (throwException) {
      if (!event.isOriginRemote() && !event.getOperation().isLocal()
          && (event.isFromBridgeAndVersioned() || // if this is a replayed client
          // event that already has a
          // version
              event.isFromWANAndVersioned())) { // or if this is a WAN event that
        // has been applied in another
        // system
        // we must distribute these since they will update the version information
        // in peers
        if (logger.isDebugEnabled()) {
          logger.debug("ARM.destroy is allowing wan/client destroy of {} to continue",
              event.getKey());
        }
        throwException = false;
        event.setIsRedestroyedEntry(true);
        // Distribution of this op happens on re and re might me null here before
        // distributing this destroy op.
        if (regionEntry == null) {
          regionEntry = newRegionEntry;
        }
        doPart3 = true;
      }
    }
    if (throwException) {
      if (entryNotFoundException == null) {
        throwEntryNotFound();
      } else {
        throw entryNotFoundException;
      }
    }
  }

  private void handleMissingRegionEntry() {
    // removeRecoveredEntry should be false in this case
    newRegionEntry = focusedRegionMap.getEntryFactory().createEntry(internalRegion, event.getKey(),
        Token.REMOVED_PHASE1);

    IndexManager oqlIndexManager = internalRegion.getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    try {
      synchronized (newRegionEntry) {
        oldRegionEntry = focusedRegionMap.putEntryIfAbsent(event.getKey(), newRegionEntry);
        // what is this doing?
        removeRegionEntryUntilCompleted();
        if (abortDestroyAndReturnFalse) {
          return;
        }
        if (!opCompleted) {
          // The following try has a finally that cleans up the newRegionEntry.
          // This is only needed if newRegionEntry was added to the map which only
          // happens if we didn't get completed with oldRegionEntry in the above while
          // loop.
          try {
            regionEntry = newRegionEntry;
            event.setRegionEntry(newRegionEntry);

            try {
              // if concurrency checks are enabled, destroy will set the version tag
              if (isEviction) {
                opCompleted = false;
                abortDestroyAndReturnFalse = true;
                return;
              }
              destroyEntryInternal(newRegionEntry, oldRegionEntry);
            } catch (RegionClearedException rce) {
              handleRegionClearedExceptionDuringDestroyEntryInternal(newRegionEntry);

            } catch (ConcurrentCacheModificationException ccme) {
              handleConcurrentModificationException();
              throw ccme;
            }
            // Note no need for LRU work since the entry is destroyed
            // and will be removed when gii completes
          } finally {
            if (!opCompleted && !haveTombstone /* to fix bug 51583 do this for all operations */ ) {
              focusedRegionMap.removeEntry(event.getKey(), newRegionEntry, false);
            }
            if (!opCompleted && isEviction) {
              focusedRegionMap.removeEntry(event.getKey(), newRegionEntry, false);
            }
          }
        } // !opCompleted
      } // synchronized newRegionEntry
    } finally {
      if (oqlIndexManager != null) {
        oqlIndexManager.countDownIndexUpdaters();
      }
    }
  }

  private void removeRegionEntryUntilCompleted() {
    while (!opCompleted && oldRegionEntry != null) {
      synchronized (oldRegionEntry) {
        if (oldRegionEntry.isRemovedPhase2()) {
          internalRegion.getCachePerfStats().incRetries();
          focusedRegionMap.getEntryMap().remove(event.getKey(), oldRegionEntry);
          oldRegionEntry = focusedRegionMap.putEntryIfAbsent(event.getKey(), newRegionEntry);
        } else {
          event.setRegionEntry(oldRegionEntry);

          // Last transaction related eviction check. This should
          // prevent
          // transaction conflict (caused by eviction) when the entry
          // is being added to transaction state.
          if (isEviction) {
            if (!focusedRegionMap.confirmEvictionDestroy(oldRegionEntry)) {
              opCompleted = false;
              abortDestroyAndReturnFalse = true;
              return;
            }
          }
          try {
            // if concurrency checks are enabled, destroy will set the version tag
            boolean destroyed = destroyEntry(oldRegionEntry, event, inTokenMode, cacheWrite,
                expectedOldValue, false, removeRecoveredEntry);
            if (destroyed) {
              if (retainForConcurrency) {
                internalRegion.basicDestroyBeforeRemoval(oldRegionEntry, event);
              }
              internalRegion.basicDestroyPart2(oldRegionEntry, event, inTokenMode,
                  false /* conflict with clear */, duringRI, true);
              focusedRegionMap.lruEntryDestroy(oldRegionEntry);
              doPart3 = true;
            }
          } catch (RegionClearedException rce) {
            // Ignore. The exception will ensure that we do not update
            // the LRU List
            internalRegion.basicDestroyPart2(oldRegionEntry, event, inTokenMode,
                true/* conflict with clear */, duringRI, true);
            doPart3 = true;
          } catch (ConcurrentCacheModificationException ccme) {
            handleConcurrentModificationException();
            throw ccme;
          }
          regionEntry = oldRegionEntry;
          opCompleted = true;
        }
      } // synchronized oldRegionEntry
    } // while
  }

  private void handleRegionClearedExceptionDuringDestroyEntryInternal(RegionEntry newRegionEntry) {
    // Ignore. The exception will ensure that we do not update the LRU List
    opCompleted = true;
    EntryLogger.logDestroy(event);
    internalRegion.basicDestroyPart2(newRegionEntry, event, inTokenMode, true, duringRI, true);
    doPart3 = true;
  }

  private void destroyEntryInternal(RegionEntry newRegionEntry, RegionEntry oldRegionEntry)
      throws RegionClearedException {
    opCompleted = destroyEntry(newRegionEntry, event, inTokenMode, cacheWrite, expectedOldValue,
        true, removeRecoveredEntry);
    if (opCompleted) {
      // This is a new entry that was created because we are in
      // token mode or are accepting a destroy operation by adding
      // a tombstone. There is no oldValue, so we don't need to
      // call updateSizeOnRemove
      // internalRegion.recordEvent(event);
      event.setIsRedestroyedEntry(true); // native clients need to know if the
      // entry didn't exist
      if (retainForConcurrency) {
        internalRegion.basicDestroyBeforeRemoval(oldRegionEntry, event);
      }
      internalRegion.basicDestroyPart2(newRegionEntry, event, inTokenMode, false, duringRI, true);
      doPart3 = true;
    }
  }

  private boolean destroyEntry(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean cacheWrite, @Released Object expectedOldValue, boolean forceDestroy,
      boolean removeRecoveredEntry) throws CacheWriterException, TimeoutException,
      EntryNotFoundException, RegionClearedException {
    focusedRegionMap.processVersionTag(re, event);
    final int oldSize = internalRegion.calculateRegionEntryValueSize(re);
    final boolean wasRemoved = re.isDestroyedOrRemoved();
    boolean retVal = re.destroy(event.getRegion(), event, inTokenMode, cacheWrite, expectedOldValue,
        forceDestroy, removeRecoveredEntry);
    if (retVal) {
      EntryLogger.logDestroy(event);
      if (!wasRemoved) {
        internalRegion.updateSizeOnRemove(event.getKey(), oldSize);
      }
    }
    return retVal;
  }

  private VersionTag getVersionTagFromStamp(VersionStamp stamp) {
    VersionTag tag = VersionTag.create(stamp.getMemberID());
    tag.setEntryVersion(stamp.getEntryVersion());
    tag.setRegionVersion(stamp.getRegionVersion());
    tag.setVersionTimeStamp(stamp.getVersionTimeStamp());
    tag.setDistributedSystemId(stamp.getDistributedSystemId());
    return tag;
  }

}
