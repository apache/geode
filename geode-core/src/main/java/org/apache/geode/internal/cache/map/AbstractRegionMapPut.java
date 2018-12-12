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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.logging.LogService;

public abstract class AbstractRegionMapPut {
  private static final Logger logger = LogService.getLogger();

  private final InternalRegion owner;
  private final FocusedRegionMap focusedRegionMap;
  private final EntryEventImpl event;
  private final boolean ownerInitialized;

  private boolean clearOccurred;
  private long lastModifiedTime;
  private RegionEntry regionEntry;
  private boolean create;
  private boolean completed;

  public AbstractRegionMapPut(FocusedRegionMap focusedRegionMap, InternalRegion owner,
      EntryEventImpl event) {
    this.focusedRegionMap = focusedRegionMap;
    this.owner = owner;
    this.event = event;
    this.ownerInitialized = owner.isInitialized();
  }

  protected boolean isOwnerInitialized() {
    return ownerInitialized;
  }

  protected boolean isClearOccurred() {
    return clearOccurred;
  }

  protected void setClearOccurred(boolean v) {
    clearOccurred = v;
  }

  protected long getLastModifiedTime() {
    return lastModifiedTime;
  }

  protected void setLastModifiedTime(long v) {
    lastModifiedTime = v;
  }

  protected RegionEntry getRegionEntry() {
    return regionEntry;
  }

  private void setRegionEntry(RegionEntry v) {
    regionEntry = v;
  }

  /**
   * @return true if put created a new entry; false if it updated an existing one.
   */
  protected boolean isCreate() {
    return create;
  }

  private void setCreate(boolean v) {
    create = v;
  }

  protected EntryEventImpl getEvent() {
    return event;
  }

  protected boolean isCompleted() {
    return completed;
  }

  private void setCompleted(boolean b) {
    completed = b;
  }

  protected InternalRegion getOwner() {
    return owner;
  }

  protected FocusedRegionMap getRegionMap() {
    return focusedRegionMap;
  }

  protected abstract boolean isOnlyExisting();

  protected abstract boolean entryExists(RegionEntry regionEntry);

  protected abstract void serializeNewValueIfNeeded();

  protected void runWhileLockedForCacheModification(Runnable r) {
    final boolean locked = getOwner().lockWhenRegionIsInitializing();
    try {
      r.run();
    } finally {
      if (locked) {
        getOwner().unlockWhenRegionIsInitializing();
      }
    }
  }

  protected abstract void setOldValueForDelta();

  protected abstract void setOldValueInEvent();

  protected abstract void unsetOldValueForDelta();

  protected abstract boolean checkPreconditions();

  protected abstract void invokeCacheWriter();

  protected abstract void createOrUpdateEntry();

  /**
   * Returns true if getRegionEntry should be removed from the map
   * because the put did not complete.
   * Precondition: isCreate()
   */
  protected abstract boolean shouldCreatedEntryBeRemoved();

  /**
   * Called after the put is done but before setCompleted(true)
   * is called.
   * Note that the RegionEntry that was modified by the put
   * is still synchronized when this is called.
   */
  protected abstract void doBeforeCompletionActions();

  /**
   * Called after the put is done.
   * Always called, even if the put failed.
   * Note that the RegionEntry that was modified by the put
   * is no longer synchronized when this is called.
   */
  protected abstract void doAfterCompletionActions();

  /**
   * @return regionEntry if put completed, otherwise null.
   */
  public RegionEntry put() {
    serializeNewValueIfNeeded();
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
      doAfterCompletionActions();
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
    final IndexManager oqlIndexManager = getOwner().getIndexManager();
    if (oqlIndexManager != null) {
      oqlIndexManager.waitForIndexInit();
    }
    return oqlIndexManager;
  }

  private void doPutRetryingIfNeeded() {
    do {
      if (!findAndSaveExistingEntry()) {
        return;
      }
      createNewEntryIfNeeded();
    } while (!addRegionEntryToMapAndDoPut());
  }

  /**
   * If an existing one is found, save it by calling setRegionEntry.
   *
   * @return false if an existing entry was not found and this put requires
   *         an existing one; otherwise returns true.
   */
  private boolean findAndSaveExistingEntry() {
    RegionEntry re = getRegionMap().getEntry(getEvent());
    if (isOnlyExisting() && !entryExists(re)) {
      setRegionEntry(null);
      return false;
    }
    setRegionEntry(re);
    return true;
  }

  private void createNewEntryIfNeeded() {
    setCreate(getRegionEntry() == null);
    if (isCreate()) {
      final Object key = getEvent().getKey();
      RegionEntry newEntry =
          getRegionMap().getEntryFactory().createEntry(getOwner(), key, Token.REMOVED_PHASE1);
      setRegionEntry(newEntry);
    }
  }

  /**
   * @return false if caller should retry
   */
  private boolean addRegionEntryToMapAndDoPut() {
    synchronized (getRegionEntry()) {
      putIfAbsentNewEntry();
      return doPutOnRegionEntryInMap();
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
   * @return false if caller should retry because entry is no longer in the map
   */
  private boolean doPutOnRegionEntryInMap() {
    synchronized (getRegionEntry()) {
      if (isRegionEntryRemoved()) {
        return false;
      }
      doPutOnSynchronizedRegionEntry();
      return true;
    }
  }

  private void doPutOnSynchronizedRegionEntry() {
    setOldValueForDelta();
    try {
      setOldValueInEvent();
      doPutIfPreconditionsSatisified();
    } finally {
      unsetOldValueForDelta();
      if (isCreate() && shouldCreatedEntryBeRemoved()) {
        getRegionMap().removeEntry(getEvent().getKey(), getRegionEntry(), false);
      }
    }
  }

  private void doPutIfPreconditionsSatisified() {
    if (!checkPreconditions()) {
      return;
    }
    invokeCacheWriter();
    runWithIndexUpdatingInProgress(this::doPutAndDeliverEvent);
  }

  private void doPutAndDeliverEvent() {
    createOrUpdateEntry();
    doBeforeCompletionActions();
    setCompleted(true);
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

  /**
   * @return true if the entry is in the final stage of removal
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
}
