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
import org.apache.geode.internal.cache.lru.LRUClockNode;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
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
public class VMThinDiskLRURegionEntryHeapStringKey1 extends VMThinDiskLRURegionEntryHeap {
  public VMThinDiskLRURegionEntryHeapStringKey1(RegionEntryContext context, String key,
      Object value, boolean byteEncode) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
    // caller has already confirmed that key.length <= MAX_INLINE_STRING_KEY
    long tmpBits1 = 0L;
    if (byteEncode) {
      for (int i = key.length() - 1; i >= 0; i--) {
        // Note: we know each byte is <= 0x7f so the "& 0xff" is not needed. But I added it in to
        // keep findbugs happy.
        tmpBits1 |= (byte) key.charAt(i) & 0xff;
        tmpBits1 <<= 8;
      }
      tmpBits1 |= 1 << 6;
    } else {
      for (int i = key.length() - 1; i >= 0; i--) {
        tmpBits1 |= key.charAt(i);
        tmpBits1 <<= 16;
      }
    }
    tmpBits1 |= key.length();
    this.bits1 = tmpBits1;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinDiskLRURegionEntryHeapStringKey1> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMThinDiskLRURegionEntryHeapStringKey1.class,
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
  protected void initialize(RegionEntryContext drs, Object value) {
    boolean isBackup;
    if (drs instanceof LocalRegion) {
      isBackup = ((LocalRegion) drs).getDiskRegion().isBackup();
    } else if (drs instanceof PlaceHolderDiskRegion) {
      isBackup = true;
    } else {
      throw new IllegalArgumentException("expected a LocalRegion or PlaceHolderDiskRegion");
    }
    // Delay the initialization of DiskID if overflow only
    if (isBackup) {
      diskInitialize(drs, value);
    }
  }

  @Override
  public final synchronized int updateAsyncEntrySize(EnableLRU capacityController) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize(getKeyForSizing(), null);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
    return delta;
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
  // lru code
  @Override
  public void setDelayedDiskId(LocalRegion r) {
    DiskStoreImpl ds = r.getDiskStore();
    long maxOplogSize = ds.getMaxOplogSize();
    this.id = DiskId.createDiskId(maxOplogSize, false /* over flow only */, ds.needsLinkedList());
  }

  public final synchronized int updateEntrySize(EnableLRU capacityController) {
    return updateEntrySize(capacityController, _getValue()); // OFHEAP: _getValue ok w/o incing
                                                             // refcount because we are synced and
                                                             // only getting the size
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public final synchronized int updateEntrySize(EnableLRU capacityController, Object value) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize(getKeyForSizing(), value);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
    // if ( debug ) log( "updateEntrySize key=" + getKey()
    // + (_getValue() == Token.INVALID ? " invalid" :
    // (_getValue() == Token.LOCAL_INVALID ? "local_invalid" :
    // (_getValue()==null ? " evicted" : " valid")))
    // + " oldSize=" + oldSize
    // + " newSize=" + this.size );
    return delta;
  }

  public final boolean testRecentlyUsed() {
    return areAnyBitsSet(RECENTLY_USED);
  }

  @Override
  public final void setRecentlyUsed() {
    setBits(RECENTLY_USED);
  }

  public final void unsetRecentlyUsed() {
    clearBits(~RECENTLY_USED);
  }

  public final boolean testEvicted() {
    return areAnyBitsSet(EVICTED);
  }

  public final void setEvicted() {
    setBits(EVICTED);
  }

  public final void unsetEvicted() {
    clearBits(~EVICTED);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private LRUClockNode nextLRU;
  private LRUClockNode prevLRU;
  private int size;

  public final void setNextLRUNode(LRUClockNode next) {
    this.nextLRU = next;
  }

  public final LRUClockNode nextLRUNode() {
    return this.nextLRU;
  }

  public final void setPrevLRUNode(LRUClockNode prev) {
    this.prevLRU = prev;
  }

  public final LRUClockNode prevLRUNode() {
    return this.prevLRU;
  }

  public final int getEntrySize() {
    return this.size;
  }

  protected final void setEntrySize(int size) {
    this.size = size;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // @Override
  // public StringBuilder appendFieldsToString(final StringBuilder sb) {
  // StringBuilder result = super.appendFieldsToString(sb);
  // result.append("; prev=").append(this.prevLRU==null?"null":"not null");
  // result.append("; next=").append(this.nextLRU==null?"null":"not null");
  // return result;
  // }
  @Override
  public Object getKeyForSizing() {
    // inline keys always report null for sizing since the size comes from the entry size
    return null;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  private final long bits1;

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
    if (getEncoding() == 1) {
      for (int i = 0; i < keylen; i++) {
        tmpBits1 >>= 8;
        chars[i] = (char) (tmpBits1 & 0x00ff);
      }
    } else {
      for (int i = 0; i < keylen; i++) {
        tmpBits1 >>= 16;
        chars[i] = (char) (tmpBits1 & 0x00FFff);
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
        if (getEncoding() == 1) {
          for (int i = 0; i < keylen; i++) {
            tmpBits1 >>= 8;
            char c = (char) (tmpBits1 & 0x00ff);
            if (str.charAt(i) != c) {
              return false;
            }
          }
        } else {
          for (int i = 0; i < keylen; i++) {
            tmpBits1 >>= 16;
            char c = (char) (tmpBits1 & 0x00FFff);
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
