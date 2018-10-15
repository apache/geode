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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

class LRUTestEntry implements EvictableEntry {

  private int id;
  private EvictionNode next;
  private EvictionNode prev;
  private int size;
  private boolean recentlyUsed;
  private boolean evicted;

  public LRUTestEntry(int id) {
    this.id = id;
    next = null;
    prev = null;
    size = 0;
    recentlyUsed = false;
    evicted = false;
  }

  public int id() {
    return id;
  }

  public boolean isTombstone() {
    return false;
  }

  @Override
  public boolean fillInValue(final InternalRegion region, final InitialImageOperation.Entry entry,
      final ByteArrayDataInput in, final DistributionManager distributionManager,
      final Version version) {
    return false;
  }

  @Override
  public boolean isOverflowedToDisk(final InternalRegion region,
      final DistributedRegion.DiskPosition diskPosition) {
    return false;
  }

  @Override
  public Object getKey() {
    return null;
  }

  @Override
  public Object getValue(final RegionEntryContext context) {
    return null;
  }

  @Override
  public Object getValueRetain(final RegionEntryContext context) {
    return null;
  }

  @Override
  public void setValue(final RegionEntryContext context, final Object value)
      throws RegionClearedException {

  }

  @Override
  public void setValue(final RegionEntryContext context, final Object value,
      final EntryEventImpl event) throws RegionClearedException {

  }

  @Override
  public Object getValueRetain(final RegionEntryContext context, final boolean decompress) {
    return null;
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public Token getValueAsToken() {
    return null;
  }

  @Override
  public void setValueWithTombstoneCheck(final Object value, final EntryEvent event)
      throws RegionClearedException {

  }

  @Override
  public Object getTransformedValue() {
    return null;
  }

  @Override
  public Object getValueInVM(final RegionEntryContext context) {
    return null;
  }

  @Override
  public Object getValueOnDisk(final InternalRegion region) throws EntryNotFoundException {
    return null;
  }

  @Override
  public Object getValueOnDiskOrBuffer(final InternalRegion region) throws EntryNotFoundException {
    return null;
  }

  @Override
  public boolean initialImagePut(final InternalRegion region, final long lastModified,
      final Object newValue, final boolean wasRecovered, final boolean acceptedVersionTag)
      throws RegionClearedException {
    return false;
  }

  @Override
  public boolean initialImageInit(final InternalRegion region, final long lastModified,
      final Object newValue, final boolean create, final boolean wasRecovered,
      final boolean acceptedVersionTag) throws RegionClearedException {
    return false;
  }

  @Override
  public boolean destroy(final InternalRegion region, final EntryEventImpl event,
      final boolean inTokenMode, final boolean cacheWrite, final Object expectedOldValue,
      final boolean forceDestroy, final boolean removeRecoveredEntry) throws CacheWriterException,
      EntryNotFoundException, TimeoutException, RegionClearedException {
    return false;
  }

  @Override
  public boolean getValueWasResultOfSearch() {
    return false;
  }

  @Override
  public void setValueResultOfSearch(final boolean value) {

  }

  @Override
  public Object getSerializedValueOnDisk(final InternalRegion region) {
    return null;
  }

  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(final InternalRegion region) {
    return null;
  }

  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(final InternalRegion region) {
    return null;
  }

  @Override
  public boolean isUpdateInProgress() {
    return false;
  }

  @Override
  public void setUpdateInProgress(final boolean underUpdate) {

  }

  @Override
  public boolean isCacheListenerInvocationInProgress() {
    return false;
  }

  @Override
  public void setCacheListenerInvocationInProgress(final boolean isListenerInvoked) {

  }

  @Override
  public boolean isValueNull() {
    return false;
  }

  @Override
  public boolean isInvalid() {
    return false;
  }

  @Override
  public boolean isDestroyed() {
    return false;
  }

  @Override
  public boolean isDestroyedOrRemoved() {
    return false;
  }

  @Override
  public boolean isDestroyedOrRemovedButNotTombstone() {
    return false;
  }

  @Override
  public boolean isInvalidOrRemoved() {
    return false;
  }

  @Override
  public void setValueToNull() {

  }

  @Override
  public void returnToPool() {

  }

  @Override
  public void setNext(EvictionNode next) {
    this.next = next;
  }

  @Override
  public EvictionNode next() {
    return this.next;
  }

  @Override
  public void setPrevious(EvictionNode previous) {
    this.prev = previous;
  }

  @Override
  public EvictionNode previous() {
    return this.prev;
  }

  @Override
  public int updateEntrySize(EvictionController cc) {
    return this.size = 1;
  }

  @Override
  public int updateEntrySize(EvictionController cc, Object value) {
    return this.size = 1;
  }

  @Override
  public int getEntrySize() {
    return this.size;
  }

  /** this should only happen with the LRUClockHand sync'ed */
  @Override
  public void setEvicted() {
    evicted = true;
  }

  @Override
  public void unsetEvicted() {
    evicted = false;
  }

  @Override
  public boolean isEvicted() {
    return evicted;
  }

  @Override
  public boolean isRecentlyUsed() {
    return recentlyUsed;
  }

  @Override
  public void setRecentlyUsed(final RegionEntryContext context) {
    recentlyUsed = true;
    context.incRecentlyUsed();
  }

  @Override
  public long getLastModified() {
    return 0;
  }

  @Override
  public boolean hasStats() {
    return false;
  }

  @Override
  public long getLastAccessed() throws InternalStatisticsDisabledException {
    return 0;
  }

  @Override
  public long getHitCount() throws InternalStatisticsDisabledException {
    return 0;
  }

  @Override
  public long getMissCount() throws InternalStatisticsDisabledException {
    return 0;
  }

  @Override
  public void updateStatsForPut(final long lastModifiedTime, final long lastAccessedTime) {

  }

  @Override
  public VersionStamp getVersionStamp() {
    return null;
  }

  @Override
  public VersionTag generateVersionTag(final VersionSource member, final boolean withDelta,
      final InternalRegion region, final EntryEventImpl event) {
    return null;
  }

  @Override
  public boolean dispatchListenerEvents(final EntryEventImpl event) throws InterruptedException {
    return false;
  }

  @Override
  public void updateStatsForGet(final boolean hit, final long time) {

  }

  @Override
  public void txDidDestroy(final long currentTime) {

  }

  @Override
  public void resetCounts() throws InternalStatisticsDisabledException {

  }

  @Override
  public void makeTombstone(final InternalRegion region, final VersionTag version)
      throws RegionClearedException {

  }

  @Override
  public void removePhase1(final InternalRegion region, final boolean clear)
      throws RegionClearedException {

  }

  @Override
  public void removePhase2() {

  }

  @Override
  public boolean isRemoved() {
    return false;
  }

  @Override
  public boolean isRemovedPhase2() {
    return false;
  }

  @Override
  public void unsetRecentlyUsed() {
    recentlyUsed = false;
  }

  public EvictionNode absoluteSelf() {
    return this;
  }

  public EvictionNode clearClones() {
    return this;
  }

  public int cloneCount() {
    return 0;
  }

  @Override
  public boolean isInUseByTransaction() {
    return false;
  }

  @Override
  public void incRefCount() {

  }

  @Override
  public void decRefCount(final EvictionList lruList, final InternalRegion region) {

  }

  @Override
  public void resetRefCount(final EvictionList lruList) {

  }

  @Override
  public Object prepareValueForCache(final RegionEntryContext context, final Object value,
      final boolean isEntryUpdate) {
    return null;
  }

  @Override
  public Object prepareValueForCache(final RegionEntryContext context, final Object value,
      final EntryEventImpl event, final boolean isEntryUpdate) {
    return null;
  }

  @Override
  public Object getKeyForSizing() {
    return null;
  }

  @Override
  public void setDelayedDiskId(final DiskRecoveryStore diskRecoveryStore) {

  }
}
