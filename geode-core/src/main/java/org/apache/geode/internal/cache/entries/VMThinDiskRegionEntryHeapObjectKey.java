

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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.eviction.EvictionController;
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
public class VMThinDiskRegionEntryHeapObjectKey extends VMThinDiskRegionEntryHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VMThinDiskRegionEntryHeapObjectKey> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VMThinDiskRegionEntryHeapObjectKey.class, "lastModified");
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
  // --------------------------------------- key fields -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private final Object key;

  public VMThinDiskRegionEntryHeapObjectKey(final RegionEntryContext context, final Object key,
      final Object value) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
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

  // ----------------------------------------- disk code ------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  protected void initialize(final RegionEntryContext context, final Object value) {
    diskInitialize(context, value);
  }

  @Override
  public int updateAsyncEntrySize(final EvictionController evictionController) {
    throw new IllegalStateException("should never be called");
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public DiskId getDiskId() {
    return id;
  }

  @Override
  public void setDiskId(final RegionEntry oldEntry) {
    id = ((DiskEntry) oldEntry).getDiskId();
  }

  private void diskInitialize(final RegionEntryContext context, final Object value) {
    DiskRecoveryStore diskRecoveryStore = (DiskRecoveryStore) context;
    DiskStoreImpl diskStore = diskRecoveryStore.getDiskStore();
    long maxOplogSize = diskStore.getMaxOplogSize();
    // get appropriate instance of DiskId implementation based on maxOplogSize
    id = DiskId.createDiskId(maxOplogSize, true, diskStore.needsLinkedList());
    Helper.initialize(this, diskRecoveryStore, value);
  }

  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Object getKey() {
    return key;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
