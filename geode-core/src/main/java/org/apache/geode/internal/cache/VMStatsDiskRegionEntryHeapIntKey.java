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

// DO NOT modify this class. It was generated from LeafRegionEntry.cpp



import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;

import org.apache.geode.internal.InternalStatisticsDisabledException;

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
public class VMStatsDiskRegionEntryHeapIntKey extends VMStatsDiskRegionEntryHeap {

  public VMStatsDiskRegionEntryHeapIntKey(final RegionEntryContext context, final int key,



      final Object value



  ) {
    super(context,

        (value instanceof RecoveredEntry ? null : value)



    );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

    initialize(context, value);



    this.key = key;

  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMStatsDiskRegionEntryHeapIntKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapIntKey.class, "lastModified");

  private volatile Object value;

  @Override
  protected Object getValueField() {
    return this.value;
  }

  @Override
  protected void setValueField(final Object value) {
    this.value = value;
  }


  protected long getLastModifiedField() {
    return lastModifiedUpdater.get(this);
  }

  protected boolean compareAndSetLastModifiedField(final long expectedValue, final long newValue) {
    return lastModifiedUpdater.compareAndSet(this, expectedValue, newValue);
  }

  @Override
  public int getEntryHash() {
    return this.hash;
  }

  protected void setEntryHash(final int hash) {
    this.hash = hash;
  }

  @Override
  public HashEntry<Object, Object> getNextEntry() {
    return this.next;
  }

  @Override
  public void setNextEntry(final HashEntry<Object, Object> next) {
    this.next = next;
  }


  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // disk code


  protected void initialize(final RegionEntryContext context, final Object value) {
    diskInitialize(context, value);
  }

  @Override
  public int updateAsyncEntrySize(final EnableLRU capacityController) {
    throw new IllegalStateException("should never be called");
  }


  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  private void diskInitialize(final RegionEntryContext context, final Object value) {
    DiskRecoveryStore diskRecoveryStore = (DiskRecoveryStore) context;
    DiskStoreImpl diskStore = diskRecoveryStore.getDiskStore();
    long maxOplogSize = diskStore.getMaxOplogSize();
    // get appropriate instance of DiskId implementation based on maxOplogSize
    this.id = DiskId.createDiskId(maxOplogSize, true, diskStore.needsLinkedList());
    Helper.initialize(this, diskRecoveryStore, value);
  }

  /**
   * @since GemFire 5.1
   */
  protected DiskId id;

  @Override
  public DiskId getDiskId() {
    return this.id;
  }

  @Override
  void setDiskId(final RegionEntry oldEntry) {
    this.id = ((AbstractDiskRegionEntry) oldEntry).getDiskId();
  }



  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // stats code

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
    setLastModified(lastModified);
    if (!DISABLE_ACCESS_TIME_UPDATE_ON_PUT) {
      setLastAccessed(lastAccessed);
    }
  }

  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;

  private static final AtomicIntegerFieldUpdater<VMStatsDiskRegionEntryHeapIntKey> hitCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapIntKey.class, "hitCount");

  private static final AtomicIntegerFieldUpdater<VMStatsDiskRegionEntryHeapIntKey> missCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapIntKey.class, "missCount");

  @Override
  public long getLastAccessed() throws InternalStatisticsDisabledException {
    return this.lastAccessed;
  }

  private void setLastAccessed(final long lastAccessed) {
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
    hitCountUpdater.incrementAndGet(this);
  }

  private void incrementMissCount() {
    missCountUpdater.incrementAndGet(this);
  }

  @Override
  public void resetCounts() throws InternalStatisticsDisabledException {
    hitCountUpdater.set(this, 0);
    missCountUpdater.set(this, 0);
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



  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // key code


  private final int key;

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

