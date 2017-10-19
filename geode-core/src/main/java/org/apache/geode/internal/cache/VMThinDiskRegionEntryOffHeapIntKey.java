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



import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;

import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

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
public class VMThinDiskRegionEntryOffHeapIntKey extends VMThinDiskRegionEntryOffHeap {

  public VMThinDiskRegionEntryOffHeapIntKey(final RegionEntryContext context, final int key,

      @Retained

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
  private static final AtomicLongFieldUpdater<VMThinDiskRegionEntryOffHeapIntKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMThinDiskRegionEntryOffHeapIntKey.class, "lastModified");

  /**
   * All access done using offHeapAddressUpdater so it is used even though the compiler can not tell
   * it is.
   */
  @SuppressWarnings("unused")
  @Retained
  @Released
  private volatile long offHeapAddress;
  /**
   * I needed to add this because I wanted clear to call setValue which normally can only be called
   * while the re is synced. But if I sync in that code it causes a lock ordering deadlock with the
   * disk regions because they also get a rw lock in clear. Some hardware platforms do not support
   * CAS on a long. If gemfire is run on one of those the AtomicLongFieldUpdater does a sync on the
   * re and we will once again be deadlocked. I don't know if we support any of the hardware
   * platforms that do not have a 64bit CAS. If we do then we can expect deadlocks on disk regions.
   */
  private final static AtomicLongFieldUpdater<VMThinDiskRegionEntryOffHeapIntKey> offHeapAddressUpdater =
      AtomicLongFieldUpdater.newUpdater(VMThinDiskRegionEntryOffHeapIntKey.class, "offHeapAddress");

  @Override
  public Token getValueAsToken() {
    return OffHeapRegionEntryHelper.getValueAsToken(this);
  }

  @Override
  protected Object getValueField() {
    return OffHeapRegionEntryHelper._getValue(this);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  @Override

  @Unretained
  protected void setValueField(@Unretained final Object value) {



    OffHeapRegionEntryHelper.setValue(this, value);
  }

  @Override

  @Retained

  public Object getValueRetain(final RegionEntryContext context, final boolean decompress) {
    return OffHeapRegionEntryHelper._getValueRetain(this, decompress, context);
  }

  @Override
  public long getAddress() {
    return offHeapAddressUpdater.get(this);
  }

  @Override
  public boolean setAddress(final long expectedAddress, long newAddress) {
    return offHeapAddressUpdater.compareAndSet(this, expectedAddress, newAddress);
  }

  @Override

  @Released

  public void release() {
    OffHeapRegionEntryHelper.releaseEntry(this);
  }

  @Override
  public void returnToPool() {
    // never implemented
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

