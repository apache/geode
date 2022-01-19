

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

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.eviction.EvictionController;
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
public class VersionedStatsDiskRegionEntryOffHeapObjectKey
    extends VersionedStatsDiskRegionEntryOffHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VersionedStatsDiskRegionEntryOffHeapObjectKey> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryOffHeapObjectKey.class,
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
  private static final AtomicLongFieldUpdater<VersionedStatsDiskRegionEntryOffHeapObjectKey> OFF_HEAP_ADDRESS_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryOffHeapObjectKey.class,
          "offHeapAddress");
  // ---------------------------------------- disk fields -----------------------------------------
  /**
   * @since GemFire 5.1
   */
  protected DiskId id;
  // --------------------------------------- stats fields -----------------------------------------
  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;
  private static final AtomicIntegerFieldUpdater<VersionedStatsDiskRegionEntryOffHeapObjectKey> HIT_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryOffHeapObjectKey.class,
          "hitCount");
  private static final AtomicIntegerFieldUpdater<VersionedStatsDiskRegionEntryOffHeapObjectKey> MISS_COUNT_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryOffHeapObjectKey.class,
          "missCount");
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
  private final Object key;

  public VersionedStatsDiskRegionEntryOffHeapObjectKey(final RegionEntryContext context,
      final Object key, @Retained final Object value) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
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
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
