

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
import org.apache.geode.internal.cache.RegionEntryContext;
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
public class VMStatsRegionEntryHeapObjectKey extends VMStatsRegionEntryHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VMStatsRegionEntryHeapObjectKey> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "lastModified");
  protected int hash;
  private HashEntry<Object, Object> nextEntry;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private volatile Object value;
  // --------------------------------------- stats fields -----------------------------------------
  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryHeapObjectKey> HIT_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "hitCount");
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryHeapObjectKey> MISS_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "missCount");
  // --------------------------------------- key fields -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private final Object key;

  public VMStatsRegionEntryHeapObjectKey(final RegionEntryContext context, final Object key,
      final Object value) {
    super(context, value);
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  protected Object getValueField() {
    return value;
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
    return hash;
  }

  @Override
  protected void setEntryHash(final int hash) {
    this.hash = hash;
  }

  @Override
  public HashEntry<Object, Object> getNextEntry() {
    return nextEntry;
  }

  @Override
  public void setNextEntry(final HashEntry<Object, Object> nextEntry) {
    this.nextEntry = nextEntry;
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
    return lastAccessed;
  }

  @Override
  public void setLastAccessed(final long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  @Override
  public long getHitCount() throws InternalStatisticsDisabledException {
    return hitCount & 0xFFFFFFFFL;
  }

  @Override
  public long getMissCount() throws InternalStatisticsDisabledException {
    return missCount & 0xFFFFFFFFL;
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
    hitCount = 0;
    missCount = 0;
  }

  @Override
  public boolean hasStats() {
    return true;
  }

  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Object getKey() {
    return key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
