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
package org.apache.geode.internal.cache.entries;

// DO NOT modify this class. It was generated from LeafRegionEntry.cpp
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.lru.LRUClockNode;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

/*
 * macros whose definition changes this class:
 *
 * disk: DISK lru: LRU stats: STATS versioned: VERSIONED offheap: OFFHEAP
 *
 * One of the following key macros must be defined:
 *
 * key object: KEY_OBJECT key int: KEY_INT key long: KEY_LONG key uuid: KEY_UUID key string1:
 * KEY_STRING1 key string2: KEY_STRING2
 */
/**
 * Do not modify this class. It was generated. Instead modify LeafRegionEntry.cpp and then run
 * ./dev-tools/generateRegionEntryClasses.sh (it must be run from the top level directory).
 */
public class VMStatsDiskLRURegionEntryHeapIntKey extends VMStatsDiskLRURegionEntryHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VMStatsDiskLRURegionEntryHeapIntKey> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VMStatsDiskLRURegionEntryHeapIntKey.class, "lastModified");
  protected int hash;
  private HashEntry<Object, Object> nextEntry;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private volatile Object value;
  // ---------------------------------------- disk fields -----------------------------------------
  /**
   * @since GemFire 5.1
   */
  protected DiskId id;
  // --------------------------------------- stats fields -----------------------------------------
  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;
  private static final AtomicIntegerFieldUpdater<VMStatsDiskLRURegionEntryHeapIntKey> HIT_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskLRURegionEntryHeapIntKey.class, "hitCount");
  private static final AtomicIntegerFieldUpdater<VMStatsDiskLRURegionEntryHeapIntKey> MISS_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskLRURegionEntryHeapIntKey.class, "missCount");
  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private final int key;

  public VMStatsDiskLRURegionEntryHeapIntKey(final RegionEntryContext context, final int key,
      final Object value) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  protected Object getValueField() {
    return this.value;
  }

  @Override
  protected void setValueField(final Object value) {
    this.value = value;
  }

  @Override
  protected long getLastModifiedField() {
    return LAST_MODIFIED_UPDATER.get(this);
  }

  @Override
  protected boolean compareAndSetLastModifiedField(final long expectedValue, final long newValue) {
    return LAST_MODIFIED_UPDATER.compareAndSet(this, expectedValue, newValue);
  }

  @Override
  public int getEntryHash() {
    return this.hash;
  }

  @Override
  protected void setEntryHash(final int hash) {
    this.hash = hash;
  }

  @Override
  public HashEntry<Object, Object> getNextEntry() {
    return this.nextEntry;
  }

  @Override
  public void setNextEntry(final HashEntry<Object, Object> nextEntry) {
    this.nextEntry = nextEntry;
  }

  // ----------------------------------------- disk code ------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  protected void initialize(final RegionEntryContext context, final Object value) {
    boolean isBackup;
    if (context instanceof InternalRegion) {
      isBackup = ((InternalRegion) context).getDiskRegion().isBackup();
    } else if (context instanceof PlaceHolderDiskRegion) {
      isBackup = true;
    } else {
      throw new IllegalArgumentException("expected a InternalRegion or PlaceHolderDiskRegion");
    }
    // Delay the initialization of DiskID if overflow only
    if (isBackup) {
      diskInitialize(context, value);
    }
  }

  @Override
  public synchronized int updateAsyncEntrySize(final EnableLRU capacityController) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize(getKeyForSizing(), null);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
    return delta;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public DiskId getDiskId() {
    return this.id;
  }

  @Override
  public void setDiskId(final RegionEntry oldEntry) {
    this.id = ((DiskEntry) oldEntry).getDiskId();
  }

  private void diskInitialize(final RegionEntryContext context, final Object value) {
    DiskRecoveryStore diskRecoveryStore = (DiskRecoveryStore) context;
    DiskStoreImpl diskStore = diskRecoveryStore.getDiskStore();
    long maxOplogSize = diskStore.getMaxOplogSize();
    // get appropriate instance of DiskId implementation based on maxOplogSize
    this.id = DiskId.createDiskId(maxOplogSize, true, diskStore.needsLinkedList());
    Helper.initialize(this, diskRecoveryStore, value);
  }

  // --------------------------------------- eviction code ----------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public void setDelayedDiskId(final DiskRecoveryStore diskRecoveryStore) {
    DiskStoreImpl diskStore = diskRecoveryStore.getDiskStore();
    long maxOplogSize = diskStore.getMaxOplogSize();
    this.id = DiskId.createDiskId(maxOplogSize, false, diskStore.needsLinkedList());
  }

  @Override
  public synchronized int updateEntrySize(final EnableLRU capacityController) {
    // OFFHEAP: getValue ok w/o incing refcount because we are synced and only getting the size
    return updateEntrySize(capacityController, getValue());
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public synchronized int updateEntrySize(final EnableLRU capacityController, final Object value) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize(getKeyForSizing(), value);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
    return delta;
  }

  @Override
  public boolean testRecentlyUsed() {
    return areAnyBitsSet(RECENTLY_USED);
  }

  @Override
  public void setRecentlyUsed() {
    setBits(RECENTLY_USED);
  }

  @Override
  public void unsetRecentlyUsed() {
    clearBits(~RECENTLY_USED);
  }

  @Override
  public boolean testEvicted() {
    return areAnyBitsSet(EVICTED);
  }

  @Override
  public void setEvicted() {
    setBits(EVICTED);
  }

  @Override
  public void unsetEvicted() {
    clearBits(~EVICTED);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private LRUClockNode nextLRU;
  private LRUClockNode previousLRU;
  private int size;

  @Override
  public void setNextLRUNode(final LRUClockNode nextLRU) {
    this.nextLRU = nextLRU;
  }

  @Override
  public LRUClockNode nextLRUNode() {
    return this.nextLRU;
  }

  @Override
  public void setPrevLRUNode(final LRUClockNode previousLRU) {
    this.previousLRU = previousLRU;
  }

  @Override
  public LRUClockNode prevLRUNode() {
    return this.previousLRU;
  }

  @Override
  public int getEntrySize() {
    return this.size;
  }

  protected void setEntrySize(final int size) {
    this.size = size;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Object getKeyForSizing() {
    // inline keys always report null for sizing since the size comes from the entry size
    return null;
  }

  // ---------------------------------------- stats code ------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public void updateStatsForGet(final boolean isHit, final long time) {
    setLastAccessed(time);
    if (isHit) {
      incrementHitCount();
    } else {
      incrementMissCount();
    }
  }

  @Override
  protected void setLastModifiedAndAccessedTimes(final long lastModified, final long lastAccessed) {
    _setLastModified(lastModified);
    if (!DISABLE_ACCESS_TIME_UPDATE_ON_PUT) {
      setLastAccessed(lastAccessed);
    }
  }

  @Override
  public long getLastAccessed() throws InternalStatisticsDisabledException {
    return this.lastAccessed;
  }

  @Override
  public void setLastAccessed(final long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  @Override
  public long getHitCount() throws InternalStatisticsDisabledException {
    return this.hitCount & 0xFFFFFFFFL;
  }

  @Override
  public long getMissCount() throws InternalStatisticsDisabledException {
    return this.missCount & 0xFFFFFFFFL;
  }

  private void incrementHitCount() {
    HIT_COUNT_UPDATER.incrementAndGet(this);
  }

  private void incrementMissCount() {
    MISS_COUNT_UPDATER.incrementAndGet(this);
  }

  @Override
  public void resetCounts() throws InternalStatisticsDisabledException {
    HIT_COUNT_UPDATER.set(this, 0);
    MISS_COUNT_UPDATER.set(this, 0);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public void txDidDestroy(long timeStamp) {
    setLastModified(timeStamp);
    setLastAccessed(timeStamp);
    this.hitCount = 0;
    this.missCount = 0;
  }

  @Override
  public boolean hasStats() {
    return true;
  }

  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Object getKey() {
    return this.key;
  }

  @Override
  public boolean isKeyEqual(final Object key) {
    if (key instanceof Integer) {
      return ((Integer) key).intValue() == this.key;
    }
    return false;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
