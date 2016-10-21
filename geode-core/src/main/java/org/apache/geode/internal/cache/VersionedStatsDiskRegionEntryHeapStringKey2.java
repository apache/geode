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
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
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
public class VersionedStatsDiskRegionEntryHeapStringKey2 extends VersionedStatsDiskRegionEntryHeap {
  public VersionedStatsDiskRegionEntryHeapStringKey2(RegionEntryContext context, String key,
      Object value, boolean byteEncode) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
    // caller has already confirmed that key.length <= MAX_INLINE_STRING_KEY
    long tmpBits1 = 0L;
    long tmpBits2 = 0L;
    if (byteEncode) {
      for (int i = key.length() - 1; i >= 0; i--) {
        // Note: we know each byte is <= 0x7f so the "& 0xff" is not needed. But I added it in to
        // keep findbugs happy.
        if (i < 7) {
          tmpBits1 |= (byte) key.charAt(i) & 0xff;
          tmpBits1 <<= 8;
        } else {
          tmpBits2 <<= 8;
          tmpBits2 |= (byte) key.charAt(i) & 0xff;
        }
      }
      tmpBits1 |= 1 << 6;
    } else {
      for (int i = key.length() - 1; i >= 0; i--) {
        if (i < 3) {
          tmpBits1 |= key.charAt(i);
          tmpBits1 <<= 16;
        } else {
          tmpBits2 <<= 16;
          tmpBits2 |= key.charAt(i);
        }
      }
    }
    tmpBits1 |= key.length();
    this.bits1 = tmpBits1;
    this.bits2 = tmpBits2;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VersionedStatsDiskRegionEntryHeapStringKey2> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryHeapStringKey2.class,
          "lastModified");
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
  // disk code
  protected void initialize(RegionEntryContext context, Object value) {
    diskInitialize(context, value);
  }

  @Override
  public int updateAsyncEntrySize(EnableLRU capacityController) {
    throw new IllegalStateException("should never be called");
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private void diskInitialize(RegionEntryContext context, Object value) {
    DiskRecoveryStore drs = (DiskRecoveryStore) context;
    DiskStoreImpl ds = drs.getDiskStore();
    long maxOplogSize = ds.getMaxOplogSize();
    // get appropriate instance of DiskId implementation based on maxOplogSize
    this.id = DiskId.createDiskId(maxOplogSize, true/* is persistence */, ds.needsLinkedList());
    Helper.initialize(this, drs, value);
  }

  /**
   * DiskId
   * 
   * @since GemFire 5.1
   */
  protected DiskId id;// = new DiskId();

  public DiskId getDiskId() {
    return this.id;
  }

  @Override
  void setDiskId(RegionEntry old) {
    this.id = ((AbstractDiskRegionEntry) old).getDiskId();
  }

  // // inlining DiskId
  // // always have these fields
  // /**
  // * id consists of
  // * most significant
  // * 1 byte = users bits
  // * 2-8 bytes = oplog id
  // * least significant.
  // *
  // * The highest bit in the oplog id part is set to 1 if the oplog id
  // * is negative.
  // * @todo this field could be an int for an overflow only region
  // */
  // private long id;
  // /**
  // * Length of the bytes on disk.
  // * This is always set. If the value is invalid then it will be set to 0.
  // * The most significant bit is used by overflow to mark it as needing to be written.
  // */
  // protected int valueLength = 0;
  // // have intOffset or longOffset
  // // intOffset
  // /**
  // * The position in the oplog (the oplog offset) where this entry's value is
  // * stored
  // */
  // private volatile int offsetInOplog;
  // // longOffset
  // /**
  // * The position in the oplog (the oplog offset) where this entry's value is
  // * stored
  // */
  // private volatile long offsetInOplog;
  // // have overflowOnly or persistence
  // // overflowOnly
  // // no fields
  // // persistent
  // /** unique entry identifier * */
  // private long keyId;
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
  private static final AtomicIntegerFieldUpdater<VersionedStatsDiskRegionEntryHeapStringKey2> hitCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryHeapStringKey2.class,
          "hitCount");
  private static final AtomicIntegerFieldUpdater<VersionedStatsDiskRegionEntryHeapStringKey2> missCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VersionedStatsDiskRegionEntryHeapStringKey2.class,
          "missCount");

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
  // versioned code
  private VersionSource memberID;
  private short entryVersionLowBytes;
  private short regionVersionHighBytes;
  private int regionVersionLowBytes;
  private byte entryVersionHighByte;
  private byte distributedSystemId;

  public int getEntryVersion() {
    return ((entryVersionHighByte << 16) & 0xFF0000) | (entryVersionLowBytes & 0xFFFF);
  }

  public long getRegionVersion() {
    return (((long) regionVersionHighBytes) << 32) | (regionVersionLowBytes & 0x00000000FFFFFFFFL);
  }

  public long getVersionTimeStamp() {
    return getLastModified();
  }

  public void setVersionTimeStamp(long time) {
    setLastModified(time);
  }

  public VersionSource getMemberID() {
    return this.memberID;
  }

  public int getDistributedSystemId() {
    return this.distributedSystemId;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public void setVersions(VersionTag tag) {
    this.memberID = tag.getMemberID();
    int eVersion = tag.getEntryVersion();
    this.entryVersionLowBytes = (short) (eVersion & 0xffff);
    this.entryVersionHighByte = (byte) ((eVersion & 0xff0000) >> 16);
    this.regionVersionHighBytes = tag.getRegionVersionHighBytes();
    this.regionVersionLowBytes = tag.getRegionVersionLowBytes();
    if (!(tag.isGatewayTag()) && this.distributedSystemId == tag.getDistributedSystemId()) {
      if (getVersionTimeStamp() <= tag.getVersionTimeStamp()) {
        setVersionTimeStamp(tag.getVersionTimeStamp());
      } else {
        tag.setVersionTimeStamp(getVersionTimeStamp());
      }
    } else {
      setVersionTimeStamp(tag.getVersionTimeStamp());
    }
    this.distributedSystemId = (byte) (tag.getDistributedSystemId() & 0xff);
  }

  public void setMemberID(VersionSource memberID) {
    this.memberID = memberID;
  }

  @Override
  public VersionStamp getVersionStamp() {
    return this;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public VersionTag asVersionTag() {
    VersionTag tag = VersionTag.create(memberID);
    tag.setEntryVersion(getEntryVersion());
    tag.setRegionVersion(this.regionVersionHighBytes, this.regionVersionLowBytes);
    tag.setVersionTimeStamp(getVersionTimeStamp());
    tag.setDistributedSystemId(this.distributedSystemId);
    return tag;
  }

  public void processVersionTag(LocalRegion r, VersionTag tag, boolean isTombstoneFromGII,
      boolean hasDelta, VersionSource thisVM, InternalDistributedMember sender,
      boolean checkForConflicts) {
    basicProcessVersionTag(r, tag, isTombstoneFromGII, hasDelta, thisVM, sender, checkForConflicts);
  }

  @Override
  public void processVersionTag(EntryEvent cacheEvent) {
    // this keeps Eclipse happy. without it the sender chain becomes confused
    // while browsing this code
    super.processVersionTag(cacheEvent);
  }

  /** get rvv internal high byte. Used by region entries for transferring to storage */
  public short getRegionVersionHighBytes() {
    return this.regionVersionHighBytes;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  public int getRegionVersionLowBytes() {
    return this.regionVersionLowBytes;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  // strlen is encoded in lowest 6 bits (max strlen is 63)
  // character encoding info is in bits 7 and 8
  // The other bits are used to encoded character data.
  private final long bits1;
  // bits2 encodes character data
  private final long bits2;

  private int getKeyLength() {
    return (int) (this.bits1 & 0x003fL);
  }

  private int getEncoding() {
    // 0 means encoded as char
    // 1 means encoded as bytes that are all <= 0x7f;
    return (int) (this.bits1 >> 6) & 0x03;
  }

  @Override
  public final Object getKey() {
    int keylen = getKeyLength();
    char[] chars = new char[keylen];
    long tmpBits1 = this.bits1;
    long tmpBits2 = this.bits2;
    if (getEncoding() == 1) {
      for (int i = 0; i < keylen; i++) {
        if (i < 7) {
          tmpBits1 >>= 8;
          chars[i] = (char) (tmpBits1 & 0x00ff);
        } else {
          chars[i] = (char) (tmpBits2 & 0x00ff);
          tmpBits2 >>= 8;
        }
      }
    } else {
      for (int i = 0; i < keylen; i++) {
        if (i < 3) {
          tmpBits1 >>= 16;
          chars[i] = (char) (tmpBits1 & 0x00FFff);
        } else {
          chars[i] = (char) (tmpBits2 & 0x00FFff);
          tmpBits2 >>= 16;
        }
      }
    }
    return new String(chars);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  @Override
  public boolean isKeyEqual(Object k) {
    if (k instanceof String) {
      String str = (String) k;
      int keylen = getKeyLength();
      if (str.length() == keylen) {
        long tmpBits1 = this.bits1;
        long tmpBits2 = this.bits2;
        if (getEncoding() == 1) {
          for (int i = 0; i < keylen; i++) {
            char c;
            if (i < 7) {
              tmpBits1 >>= 8;
              c = (char) (tmpBits1 & 0x00ff);
            } else {
              c = (char) (tmpBits2 & 0x00ff);
              tmpBits2 >>= 8;
            }
            if (str.charAt(i) != c) {
              return false;
            }
          }
        } else {
          for (int i = 0; i < keylen; i++) {
            char c;
            if (i < 3) {
              tmpBits1 >>= 16;
              c = (char) (tmpBits1 & 0x00FFff);
            } else {
              c = (char) (tmpBits2 & 0x00FFff);
              tmpBits2 >>= 16;
            }
            if (str.charAt(i) != c) {
              return false;
            }
          }
        }
        return true;
      }
    }
    return false;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
