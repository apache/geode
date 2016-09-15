/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;
// DO NOT modify this class. It was generated from LeafRegionEntry.cpp
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.InternalStatisticsDisabledException;
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
 * Do not modify this class. It was generated.
 * Instead modify LeafRegionEntry.cpp and then run
 * bin/generateRegionEntryClasses.sh from the directory
 * that contains your build.xml.
 */
public class VMStatsLRURegionEntryHeapStringKey2 extends VMStatsLRURegionEntryHeap {
  public VMStatsLRURegionEntryHeapStringKey2 (RegionEntryContext context, String key,
      Object value
      , boolean byteEncode
      ) {
    super(context,
          value
        );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    // caller has already confirmed that key.length <= MAX_INLINE_STRING_KEY
    long tmpBits1 = 0L;
    long tmpBits2 = 0L;
    if (byteEncode) {
      for (int i=key.length()-1; i >= 0; i--) {
        // Note: we know each byte is <= 0x7f so the "& 0xff" is not needed. But I added it in to keep findbugs happy.
        if (i < 7) {
          tmpBits1 |= (byte)key.charAt(i) & 0xff;
          tmpBits1 <<= 8;
        } else {
          tmpBits2 <<= 8;
          tmpBits2 |= (byte)key.charAt(i) & 0xff;
        }
      }
      tmpBits1 |= 1<<6;
    } else {
      for (int i=key.length()-1; i >= 0; i--) {
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
  private static final AtomicLongFieldUpdater<VMStatsLRURegionEntryHeapStringKey2> lastModifiedUpdater
    = AtomicLongFieldUpdater.newUpdater(VMStatsLRURegionEntryHeapStringKey2.class, "lastModified");
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
  // lru code
  @Override
  public void setDelayedDiskId(LocalRegion r) {
  // nothing needed for LRUs with no disk
  }
  public final synchronized int updateEntrySize(EnableLRU capacityController) {
    return updateEntrySize(capacityController, _getValue()); // OFHEAP: _getValue ok w/o incing refcount because we are synced and only getting the size
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  public final synchronized int updateEntrySize(EnableLRU capacityController,
                                                Object value) {
    int oldSize = getEntrySize();
    int newSize = capacityController.entrySize( getKeyForSizing(), value);
    setEntrySize(newSize);
    int delta = newSize - oldSize;
  //   if ( debug ) log( "updateEntrySize key=" + getKey()
  //                     + (_getValue() == Token.INVALID ? " invalid" :
  //                        (_getValue() == Token.LOCAL_INVALID ? "local_invalid" :
  //                         (_getValue()==null ? " evicted" : " valid")))
  //                     + " oldSize=" + oldSize
  //                     + " newSize=" + this.size );
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
  public final void setNextLRUNode( LRUClockNode next ) {
    this.nextLRU = next;
  }
  public final LRUClockNode nextLRUNode() {
    return this.nextLRU;
  }
  public final void setPrevLRUNode( LRUClockNode prev ) {
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
//@Override
//public StringBuilder appendFieldsToString(final StringBuilder sb) {
//  StringBuilder result = super.appendFieldsToString(sb);
//  result.append("; prev=").append(this.prevLRU==null?"null":"not null");
//  result.append("; next=").append(this.nextLRU==null?"null":"not null");
//  return result;
//}
  @Override
  public Object getKeyForSizing() {
    // inline keys always report null for sizing since the size comes from the entry size
    return null;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // stats code
  @Override
  public final void updateStatsForGet(boolean hit, long time)
  {
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
  private static final AtomicIntegerFieldUpdater<VMStatsLRURegionEntryHeapStringKey2> hitCountUpdater
    = AtomicIntegerFieldUpdater.newUpdater(VMStatsLRURegionEntryHeapStringKey2.class, "hitCount");
  private static final AtomicIntegerFieldUpdater<VMStatsLRURegionEntryHeapStringKey2> missCountUpdater
    = AtomicIntegerFieldUpdater.newUpdater(VMStatsLRURegionEntryHeapStringKey2.class, "missCount");
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
    hitCountUpdater.set(this,0);
    missCountUpdater.set(this,0);
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
      for (int i=0; i < keylen; i++) {
        if (i < 7) {
          tmpBits1 >>= 8;
          chars[i] = (char) (tmpBits1 & 0x00ff);
        } else {
          chars[i] = (char) (tmpBits2 & 0x00ff);
          tmpBits2 >>= 8;
        }
      }
    } else {
      for (int i=0; i < keylen; i++) {
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
      String str = (String)k;
      int keylen = getKeyLength();
      if (str.length() == keylen) {
        long tmpBits1 = this.bits1;
        long tmpBits2 = this.bits2;
        if (getEncoding() == 1) {
          for (int i=0; i < keylen; i++) {
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
          for (int i=0; i < keylen; i++) {
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
