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

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ABSTRACT_REGION_ENTRY_FILL_IN_VALUE;
import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * Internal interface for a region entry. Note that a region is implemented with a ConcurrentHashMap
 * which has its own entry class. The "value" field of each of these entries will implement this
 * interface.
 *
 * <p>
 * Our RegionEntry does not need to keep a reference to the key. It is being referenced by the
 * Map.Entry whose value field references this RegionEntry.
 *
 * @since GemFire 3.5.1
 */
public interface RegionEntry {

  /**
   * returns the time of last modification. Prior to v7.0 this only reflected creation time of an
   * entry or the time it was last modified with an Update. In 7.0 and later releases it also
   * reflects invalidate and destroy.
   */
  long getLastModified();

  /**
   * Returns true if getLastAccessed, getHitCount, and getMissCount can be called without throwing
   * InternalStatisticsDisabledException. Otherwise returns false.
   * 
   * @since GemFire 6.0
   */
  boolean hasStats();

  long getLastAccessed() throws InternalStatisticsDisabledException;

  /**
   * Sets the entry's last accessed time if it has one.
   */
  default void setLastAccessed(long lastAccessed) {
    // do nothing by default
  }

  long getHitCount() throws InternalStatisticsDisabledException;

  long getMissCount() throws InternalStatisticsDisabledException;

  void updateStatsForPut(long lastModifiedTime, long lastAccessedTime);

  /**
   * @return the version information for this entry
   */
  VersionStamp getVersionStamp();

  /**
   * @param member the member performing the change, or null if it's this member
   * @param withDelta whether delta version information should be included
   * @param region the owner of the region entry
   * @param event the event causing this change
   * @return a new version tag for this entry (null if versions not supported)
   */
  VersionTag generateVersionTag(VersionSource member, boolean withDelta, LocalRegion region,
      EntryEventImpl event);

  /**
   * Dispatch listener events for the given entry event. If the event is from another member and
   * there are already pending events, this will merely queue the event for later dispatch by one of
   * those other events.
   * 
   * This is done within the RegionEntry synchronization lock.
   */
  boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException;

  /**
   * Used to mark an LRU entry as having been recently used.
   */
  void setRecentlyUsed();

  void updateStatsForGet(boolean hit, long time);

  /**
   * Resets any entry state as needed for a transaction that did a destroy to this entry.
   * 
   * @param currTime Current Cache Time.
   */
  void txDidDestroy(long currTime);

  void resetCounts() throws InternalStatisticsDisabledException;

  /**
   * this is done instead of removePhase2 if the entry needs to be kept for concurrency control
   * 
   * @param r the region holding the entry
   * @param version whether the operation is from another member or local
   */
  void makeTombstone(LocalRegion r, VersionTag version) throws RegionClearedException;

  /**
   * Mark this entry as being in the process of removal from the map that contains it by setting its
   * value to Token.REMOVED_PHASE1. An entry in this state may be resurrected by changing the value
   * under synchronization prior to the entry executing removePhase2.
   * 
   * @param r LocalRegion
   * @param clear - if we removing this entry as a result of a region clear.
   * @throws RegionClearedException If operation got aborted due to a clear
   * 
   */
  void removePhase1(LocalRegion r, boolean clear) throws RegionClearedException;

  /**
   * Mark this entry as having been removed from the map that contained it by setting its value to
   * Token.REMOVED_PHASE2
   */
  void removePhase2();

  /**
   * Returns true if this entry does not exist. This is true if removal has started (value ==
   * Token.REMOVED_PHASE1) or has completed (value == Token.REMOVED_PHASE2).
   */
  boolean isRemoved();

  /**
   * Returns true if this entry does not exist and will not be resurrected (value ==
   * Token.REMOVED_PHASE2)
   */
  boolean isRemovedPhase2();

  /**
   * Returns true if this entry is a tombstone, meaning that it has been removed but is being
   * retained for concurrent modification detection. Tombstones may be resurrected just like an
   * entry that is in REMOVED_PHASE1 state.
   */
  boolean isTombstone();

  /**
   * Fill in value, and isSerialized fields in this entry object (used for getInitialImage and sync
   * recovered) Also sets the lastModified time in cacheTime. Only called for DistributedRegions.
   *
   * <p>
   * This method returns tombstones so that they can be transferred with the initial image.
   *
   * @return false if map entry not found
   *
   * @see InitialImageOperation.RequestImageMessage#chunkEntries
   *
   * @since GemFire 3.2.1
   */
  boolean fillInValue(LocalRegion r, @Retained(ABSTRACT_REGION_ENTRY_FILL_IN_VALUE) Entry entry,
      ByteArrayDataInput in, DM mgr, final Version version);

  /**
   * Returns true if this entry has overflowed to disk.
   * 
   * @param dp if overflowed then the position of the value is set in dp
   */
  boolean isOverflowedToDisk(LocalRegion r, DistributedRegion.DiskPosition dp);

  /**
   * Gets the key for this entry.
   */
  Object getKey();

  /**
   * Gets the value for this entry. For DiskRegions, faults in value and returns it
   */
  Object getValue(RegionEntryContext context);

  /**
   * Just like getValue but the result may be a retained off-heap reference.
   */
  @Retained
  Object getValueRetain(RegionEntryContext context);

  @Released
  void setValue(RegionEntryContext context, @Unretained Object value) throws RegionClearedException;

  /**
   * This flavor of setValue was added so that the event could be passed down to Helper.writeToDisk.
   * The event can be used to cache the serialized form of the value we are writing. See bug 43781.
   */
  void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
      throws RegionClearedException;

  /**
   * Obtain, retain and return the value of this entry. WARNING: if a StoredObject is returned and
   * it has a refCount then the caller MUST make sure that {@link StoredObject#release()} before the
   * returned object is garbage collected.
   * 
   * This is only retained in off-heap subclasses. However, it's marked as Retained here so that
   * callers are aware that the value may be retained.
   * 
   * @param decompress if true returned value will be decompressed if it is compressed
   * @return possible OFF_HEAP_OBJECT (caller must release)
   */
  @Retained
  Object _getValueRetain(RegionEntryContext context, boolean decompress);

  /** Gets the value field of this entry. */

  @Unretained
  Object _getValue();

  /**
   * Returns a tokenized form of the value. If the value can not be represented as a token then
   * Token.NOT_A_TOKEN is returned.
   */
  Token getValueAsToken();

  /**
   * Set the value of this entry and perform checks to see if a GC task needs to be scheduled.
   * 
   * @param value the new value for this entry
   * @param event the cache event that caused this change
   * @throws RegionClearedException thrown if the region is concurrently cleared
   */
  void setValueWithTombstoneCheck(@Unretained Object value, EntryEvent event)
      throws RegionClearedException;

  /**
   * Returns the value as stored by the RegionEntry implementation. For instance, if compressed this
   * value would be the compressed form.
   * 
   * @since GemFire 8.0
   */
  @Retained
  Object getTransformedValue();

  /**
   * Returns the value of an entry as it resides in the VM.
   * 
   * @return the value or EntryEvent.NOT_AVAILABLE token if it's not in the VM or null if the entry
   *         doesn't exist.
   *
   * @see LocalRegion#getValueInVM
   */
  Object getValueInVM(RegionEntryContext context);

  /**
   * Returns the value of an entry as it resides on disk. For testing purposes only.
   *
   * @see LocalRegion#getValueOnDisk
   */
  Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException;

  /**
   * Returns the value of an entry as it resides on buffer or disk. For asynch mode a value is not
   * immediately written to the disk & so checking in asynch buffers is needed .For testing purposes
   * only.
   *
   * @see LocalRegion#getValueOnDisk
   */

  Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException;



  /**
   * Used to modify an existing RegionEntry when processing the values obtained during a
   * getInitialImage.
   */
  boolean initialImagePut(LocalRegion region, long lastModified, Object newValue,
      boolean wasRecovered, boolean acceptedVersionTag) throws RegionClearedException;

  /**
   * Used to initialize a created RegionEntry when processing the values obtained during a
   * getInitialImage.
   *
   * <p>
   * put if LOCAL_INVALID and nonexistant<br>
   * put if INVALID and nonexistant, recovered, or LOCAL_INVALID<br>
   * put if valid and nonexistant, recovered, or LOCAL_INVALID
   *
   * <p>
   * REGION_INVALIDATED: (special case)<br>
   * If the region itself has been invalidated since getInitialImage<br>
   * started, and newValue is LOCAL_INVALID or valid,<br>
   * then force creation of INVALID key if currently nonexistent<br>
   * or invalidate if current recovered.<br>
   *
   * <p>
   * must write-synchronize to protect against puts from other<br>
   * threads running this method
   */
  boolean initialImageInit(LocalRegion region, long lastModified, Object newValue, boolean create,
      boolean wasRecovered, boolean acceptedVersionTag) throws RegionClearedException;

  /**
   * @param expectedOldValue only succeed with destroy if current value is equal to expectedOldValue
   * @param forceDestroy true if caller just added this entry because we are inTokenMode and we need
   *        to "create" a destroyed entry to mark the destroy
   * @return true if destroy was done; false if not
   */
  @Released
  boolean destroy(LocalRegion region, EntryEventImpl event, boolean inTokenMode, boolean cacheWrite,
      @Unretained Object expectedOldValue, boolean forceDestroy, boolean removeRecoveredEntry)
      throws CacheWriterException, EntryNotFoundException, TimeoutException, RegionClearedException;

  /**
   * @return true if entry's value came from a netsearch
   * @since GemFire 6.5
   */
  boolean getValueWasResultOfSearch();

  /**
   * @param v true if entry's value should be marked as having been the result of a netsearch.
   * @since GemFire 6.5
   */
  void setValueResultOfSearch(boolean v);

  /**
   * Get the serialized bytes from disk. This method only looks for the value on the disk, ignoring
   * heap data.
   * 
   * @param localRegion the persistent region
   * @return the serialized value from disk
   * @since GemFire 5.7
   */
  Object getSerializedValueOnDisk(LocalRegion localRegion);

  /**
   * Gets the value for this entry. For DiskRegions, unlike {@link #getValue(RegionEntryContext)}
   * this will not fault in the value rather return a temporary copy.
   */
  Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner);

  /**
   * Gets the value for this entry. For DiskRegions, unlike {@link #getValue(RegionEntryContext)}
   * this will not fault in the value rather return a temporary copy. The value returned will be
   * kept off heap (and compressed) if possible.
   */
  @Retained
  Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner);

  /**
   * RegionEntry is underUpdate as soon as RegionEntry lock is help by an update thread to put a new
   * value for an existing key. This is used during query on the region to verify index entry with
   * current region entry.
   *
   * @return true if RegionEntry is under update during cache put operation.
   */
  boolean isUpdateInProgress();

  /**
   * Sets RegionEntry updateInProgress flag when put is happening for an existing Region.Entry.
   * Called ONLY in {@link AbstractRegionMap}.
   */
  void setUpdateInProgress(final boolean underUpdate);

  /**
   * Event containing this RegionEntry is being passed through dispatchListenerEvent for
   * CacheListeners under RegionEntry lock. This is used during deserialization for a
   * VMCacheSerializable value contained by this RegionEntry.
   * 
   * @return true if Event is being dispatched to CacheListeners.
   */
  boolean isCacheListenerInvocationInProgress();

  /**
   * Sets RegionEntry isCacheListenerInvoked flag when put is happening for a Region.Entry. Called
   * ONLY in {@link LocalRegion#dispatchListenerEvent}.
   */
  void setCacheListenerInvocationInProgress(final boolean isListenerInvoked);

  /**
   * Returns true if the entry value is null.
   */
  boolean isValueNull();

  /**
   * Returns true if the entry value is equal to {@link Token#INVALID} or
   * {@link Token#LOCAL_INVALID}.
   */
  boolean isInvalid();

  /**
   * Returns true if the entry value is equal to {@link Token#DESTROYED}.
   */
  boolean isDestroyed();

  /**
   * Returns true if the entry value is equal to {@link Token#DESTROYED} or
   * {@link Token#REMOVED_PHASE1} or {@link Token#REMOVED_PHASE2} or {@link Token#TOMBSTONE}.
   */
  boolean isDestroyedOrRemoved();

  /**
   * Returns true if the entry value is equal to {@link Token#DESTROYED} or
   * {@link Token#REMOVED_PHASE1} or {@link Token#REMOVED_PHASE2}.
   */
  boolean isDestroyedOrRemovedButNotTombstone();

  /**
   * @see Token#isInvalidOrRemoved(Object)
   */
  boolean isInvalidOrRemoved();

  /**
   * Sets the entry value to null.
   */
  void setValueToNull();

  void returnToPool();

  boolean isInUseByTransaction();

  void setInUseByTransaction(final boolean v);

  /**
   * Increments the number of transactions that are currently referencing this node.
   */
  void incRefCount();

  /**
   * Decrements the number of transactions that are currently referencing this node.
   * 
   * @param lr the local region that owns this region entry; null if no local region owner
   */
  void decRefCount(NewLRUClockHand lruList, LocalRegion lr);

  /**
   * Clear the number of transactions that are currently referencing this node and returns to LRU
   * list
   */
  void resetRefCount(NewLRUClockHand lruList);

  @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE)
  Object prepareValueForCache(RegionEntryContext r, Object val, boolean isEntryUpdate);

  @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE)
  Object prepareValueForCache(RegionEntryContext r, Object val, EntryEventImpl event,
      boolean isEntryUpdate);
}
