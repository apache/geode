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
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

// macros whose definition changes this class:
// disk: DISK
// lru: LRU
// stats: STATS
// versioned: VERSIONED
// offheap: OFFHEAP
// One of the following key macros must be defined:
// key object: KEY_OBJECT
// key int: KEY_INT
// key long: KEY_LONG
// key uuid: KEY_UUID
// key string1: KEY_STRING1
// key string2: KEY_STRING2
/**
 * Do not modify this class. It was generated. Instead modify LeafRegionEntry.cpp and then run
 * bin/generateRegionEntryClasses.sh from the directory that contains your build.xml.
 */
public class VMStatsRegionEntryHeapObjectKey extends VMStatsRegionEntryHeap {
  public VMStatsRegionEntryHeapObjectKey(RegionEntryContext context, Object key, Object value) {
    super(context, value);
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMStatsRegionEntryHeapObjectKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "lastModified");
  private volatile Object value;

  @Override
  protected final Object getValueField() {
    return this.value;
  }

  @Override
  protected void setValueField(Object v) {
    this.value = v;
  }

  protected long getlastModifiedField() {
    return lastModifiedUpdater.get(this);
  }

  protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
    return lastModifiedUpdater.compareAndSet(this, expectedValue, newValue);
  }

  /**
   * @see HashEntry#getEntryHash()
   */
  public final int getEntryHash() {
    return this.hash;
  }

  protected void setEntryHash(int v) {
    this.hash = v;
  }

  /**
   * @see HashEntry#getNextEntry()
   */
  public final HashEntry<Object, Object> getNextEntry() {
    return this.next;
  }

  /**
   * @see HashEntry#setNextEntry
   */
  public final void setNextEntry(final HashEntry<Object, Object> n) {
    this.next = n;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // stats code
  @Override
  public final void updateStatsForGet(boolean hit, long time) {
    setLastAccessed(time);
    if (hit) {
      incrementHitCount();
    } else {
      incrementMissCount();
    }
  }

  @Override
  protected final void setLastModified(long lastModified) {
    _setLastModified(lastModified);
    if (!DISABLE_ACCESS_TIME_UPDATE_ON_PUT) {
      setLastAccessed(lastModified);
    }
  }

  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryHeapObjectKey> hitCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "hitCount");
  private static final AtomicIntegerFieldUpdater<VMStatsRegionEntryHeapObjectKey> missCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsRegionEntryHeapObjectKey.class, "missCount");

  @Override
  public final long getLastAccessed() throws InternalStatisticsDisabledException {
    return this.lastAccessed;
  }

  private void setLastAccessed(long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  @Override
  public final long getHitCount() throws InternalStatisticsDisabledException {
    return this.hitCount & 0xFFFFFFFFL;
  }

  @Override
  public final long getMissCount() throws InternalStatisticsDisabledException {
    return this.missCount & 0xFFFFFFFFL;
  }

  private void incrementHitCount() {
    hitCountUpdater.incrementAndGet(this);
  }

  private void incrementMissCount() {
    missCountUpdater.incrementAndGet(this);
  }

  @Override
  public final void resetCounts() throws InternalStatisticsDisabledException {
    hitCountUpdater.set(this, 0);
    missCountUpdater.set(this, 0);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public final void txDidDestroy(long currTime) {
    setLastModified(currTime);
    setLastAccessed(currTime);
    this.hitCount = 0;
    this.missCount = 0;
  }

  @Override
  public boolean hasStats() {
    return true;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  private final Object key;

  @Override
  public final Object getKey() {
    return this.key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
