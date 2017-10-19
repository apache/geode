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



import java.util.UUID;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.geode.cache.EntryEvent;

import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;

import org.apache.geode.internal.cache.lru.LRUClockNode;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

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
public class VersionedThinLRURegionEntryHeapUUIDKey extends VersionedThinLRURegionEntryHeap {

  public VersionedThinLRURegionEntryHeapUUIDKey(final RegionEntryContext context, final UUID key,



      final Object value



  ) {
    super(context,



        value

    );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

    this.keyMostSigBits = key.getMostSignificantBits();
    this.keyLeastSigBits = key.getLeastSignificantBits();

  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VersionedThinLRURegionEntryHeapUUIDKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VersionedThinLRURegionEntryHeapUUIDKey.class,
          "lastModified");

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

  // lru code

  @Override
  public void setDelayedDiskId(final DiskRecoveryStore diskRecoveryStore) {



    // nothing needed for LRUs with no disk

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



  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // versioned code

  private VersionSource memberId;
  private short entryVersionLowBytes;
  private short regionVersionHighBytes;
  private int regionVersionLowBytes;
  private byte entryVersionHighByte;
  private byte distributedSystemId;

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
    return this.memberId;
  }

  @Override
  public int getDistributedSystemId() {
    return this.distributedSystemId;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  @Override
  public void setVersions(final VersionTag versionTag) {
    this.memberId = versionTag.getMemberID();
    int eVersion = versionTag.getEntryVersion();
    this.entryVersionLowBytes = (short) (eVersion & 0xffff);
    this.entryVersionHighByte = (byte) ((eVersion & 0xff0000) >> 16);
    this.regionVersionHighBytes = versionTag.getRegionVersionHighBytes();
    this.regionVersionLowBytes = versionTag.getRegionVersionLowBytes();

    if (!versionTag.isGatewayTag()
        && this.distributedSystemId == versionTag.getDistributedSystemId()) {
      if (getVersionTimeStamp() <= versionTag.getVersionTimeStamp()) {
        setVersionTimeStamp(versionTag.getVersionTimeStamp());
      } else {
        versionTag.setVersionTimeStamp(getVersionTimeStamp());
      }
    } else {
      setVersionTimeStamp(versionTag.getVersionTimeStamp());
    }

    this.distributedSystemId = (byte) (versionTag.getDistributedSystemId() & 0xff);
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
    tag.setRegionVersion(this.regionVersionHighBytes, this.regionVersionLowBytes);
    tag.setVersionTimeStamp(getVersionTimeStamp());
    tag.setDistributedSystemId(this.distributedSystemId);
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
    return this.regionVersionHighBytes;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  @Override
  public int getRegionVersionLowBytes() {
    return this.regionVersionLowBytes;
  }


  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // key code


  private final long keyMostSigBits;
  private final long keyLeastSigBits;

  @Override
  public Object getKey() {
    return new UUID(this.keyMostSigBits, this.keyLeastSigBits);
  }

  @Override
  public boolean isKeyEqual(final Object key) {
    if (key instanceof UUID) {
      UUID uuid = (UUID) key;
      return uuid.getLeastSignificantBits() == this.keyLeastSigBits
          && uuid.getMostSignificantBits() == this.keyMostSigBits;
    }
    return false;
  }



  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}

