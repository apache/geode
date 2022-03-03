

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

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionNode;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
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
public class VersionedThinLRURegionEntryOffHeapIntKey extends VersionedThinLRURegionEntryOffHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VersionedThinLRURegionEntryOffHeapIntKey> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VersionedThinLRURegionEntryOffHeapIntKey.class,
          "lastModified");
  protected int hash;
  private HashEntry<Object, Object> nextEntry;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  // --------------------------------------- offheap fields ---------------------------------------
  /**
   * All access done using OFF_HEAP_ADDRESS_UPDATER so it is used even though the compiler can not
   * tell it is.
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
   * RegionEntry and we will once again be deadlocked. I don't know if we support any of the
   * hardware platforms that do not have a 64bit CAS. If we do then we can expect deadlocks on disk
   * regions.
   */
  private static final AtomicLongFieldUpdater<VersionedThinLRURegionEntryOffHeapIntKey> OFF_HEAP_ADDRESS_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VersionedThinLRURegionEntryOffHeapIntKey.class,
          "offHeapAddress");
  // ------------------------------------- versioned fields ---------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private VersionSource memberId;
  private short entryVersionLowBytes;
  private short regionVersionHighBytes;
  private int regionVersionLowBytes;
  private byte entryVersionHighByte;
  private byte distributedSystemId;
  // --------------------------------------- key fields -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private final int key;

  public VersionedThinLRURegionEntryOffHeapIntKey(final RegionEntryContext context, final int key,
      @Retained final Object value) {
    super(context, value);
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
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
    return OFF_HEAP_ADDRESS_UPDATER.get(this);
  }

  @Override
  public boolean setAddress(final long expectedAddress, long newAddress) {
    return OFF_HEAP_ADDRESS_UPDATER.compareAndSet(this, expectedAddress, newAddress);
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

  // --------------------------------------- eviction code ----------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public void setDelayedDiskId(final DiskRecoveryStore diskRecoveryStore) {
    // nothing needed for LRUs with no disk
  }

  @Override
  public synchronized int updateEntrySize(final EvictionController evictionController) {
    // OFFHEAP: getValue ok w/o incing refcount because we are synced and only getting the size
    return updateEntrySize(evictionController, getValue());
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public synchronized int updateEntrySize(final EvictionController evictionController,
      final Object value) {
    int oldSize = getEntrySize();
    int newSize = evictionController.entrySize(getKeyForSizing(), value);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
    return delta;
  }

  @Override
  public boolean isRecentlyUsed() {
    return areAnyBitsSet(RECENTLY_USED);
  }

  @Override
  public void setRecentlyUsed(RegionEntryContext context) {
    if (!isRecentlyUsed()) {
      setBits(RECENTLY_USED);
      context.incRecentlyUsed();
    }
  }

  @Override
  public void unsetRecentlyUsed() {
    clearBits(~RECENTLY_USED);
  }

  @Override
  public boolean isEvicted() {
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
  private EvictionNode nextEvictionNode;
  private EvictionNode previousEvictionNode;
  private int size;

  @Override
  public void setNext(final EvictionNode nextEvictionNode) {
    this.nextEvictionNode = nextEvictionNode;
  }

  @Override
  public EvictionNode next() {
    return nextEvictionNode;
  }

  @Override
  public void setPrevious(final EvictionNode previousEvictionNode) {
    this.previousEvictionNode = previousEvictionNode;
  }

  @Override
  public EvictionNode previous() {
    return previousEvictionNode;
  }

  @Override
  public int getEntrySize() {
    return size;
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

  // -------------------------------------- versioned code ----------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public int getEntryVersion() {
    return ((entryVersionHighByte << 16) & 0xFF0000) | (entryVersionLowBytes & 0xFFFF);
  }

  @Override
  public long getRegionVersion() {
    return (((long) regionVersionHighBytes) << 32) | (regionVersionLowBytes & 0x00000000FFFFFFFFL);
  }

  @Override
  public long getVersionTimeStamp() {
    return getLastModified();
  }

  @Override
  public void setVersionTimeStamp(final long timeStamp) {
    setLastModified(timeStamp);
  }

  @Override
  public VersionSource getMemberID() {
    return memberId;
  }

  @Override
  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public void setVersions(final VersionTag versionTag) {
    memberId = versionTag.getMemberID();
    int eVersion = versionTag.getEntryVersion();
    entryVersionLowBytes = (short) (eVersion & 0xffff);
    entryVersionHighByte = (byte) ((eVersion & 0xff0000) >> 16);
    regionVersionHighBytes = versionTag.getRegionVersionHighBytes();
    regionVersionLowBytes = versionTag.getRegionVersionLowBytes();
    if (!versionTag.isGatewayTag()
        && distributedSystemId == versionTag.getDistributedSystemId()) {
      if (getVersionTimeStamp() <= versionTag.getVersionTimeStamp()) {
        setVersionTimeStamp(versionTag.getVersionTimeStamp());
      } else {
        versionTag.setVersionTimeStamp(getVersionTimeStamp());
      }
    } else {
      setVersionTimeStamp(versionTag.getVersionTimeStamp());
    }
    distributedSystemId = (byte) (versionTag.getDistributedSystemId() & 0xff);
  }

  @Override
  public void setMemberID(final VersionSource memberId) {
    this.memberId = memberId;
  }

  @Override
  public VersionStamp getVersionStamp() {
    return this;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public VersionTag asVersionTag() {
    VersionTag tag = VersionTag.create(memberId);
    tag.setEntryVersion(getEntryVersion());
    tag.setRegionVersion(regionVersionHighBytes, regionVersionLowBytes);
    tag.setVersionTimeStamp(getVersionTimeStamp());
    tag.setDistributedSystemId(distributedSystemId);
    return tag;
  }

  @Override
  public void processVersionTag(final InternalRegion region, final VersionTag versionTag,
      final boolean isTombstoneFromGII, final boolean hasDelta, final VersionSource versionSource,
      final InternalDistributedMember sender, final boolean checkForConflicts) {
    basicProcessVersionTag(region, versionTag, isTombstoneFromGII, hasDelta, versionSource, sender,
        checkForConflicts);
  }

  @Override
  public void processVersionTag(final EntryEvent cacheEvent) {
    // this keeps IDE happy. without it the sender chain becomes confused while browsing this code
    super.processVersionTag(cacheEvent);
  }

  /** get rvv internal high byte. Used by region entries for transferring to storage */
  @Override
  public short getRegionVersionHighBytes() {
    return regionVersionHighBytes;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  @Override
  public int getRegionVersionLowBytes() {
    return regionVersionLowBytes;
  }

  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public boolean isKeyEqual(final Object key) {
    if (key instanceof Integer) {
      return (Integer) key == this.key;
    }
    return false;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
