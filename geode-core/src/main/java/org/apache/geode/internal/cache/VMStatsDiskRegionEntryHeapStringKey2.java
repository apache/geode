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

import org.apache.geode.internal.InternalStatisticsDisabledException;

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
public class VMStatsDiskRegionEntryHeapStringKey2 extends VMStatsDiskRegionEntryHeap {

  public VMStatsDiskRegionEntryHeapStringKey2(final RegionEntryContext context, final String key,



      final Object value

      , final boolean byteEncode

  ) {
    super(context,

        (value instanceof RecoveredEntry ? null : value)



    );
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

    initialize(context, value);

    // caller has already confirmed that key.length <= MAX_INLINE_STRING_KEY
    long tempBits1 = 0L;
    long tempBits2 = 0L;
    if (byteEncode) {
      for (int i = key.length() - 1; i >= 0; i--) {
        // Note: we know each byte is <= 0x7f so the "& 0xff" is not needed. But I added it in to
        // keep findbugs happy.
        if (i < 7) {
          tempBits1 |= (byte) key.charAt(i) & 0xff;
          tempBits1 <<= 8;
        } else {
          tempBits2 <<= 8;
          tempBits2 |= (byte) key.charAt(i) & 0xff;
        }
      }
      tempBits1 |= 1 << 6;
    } else {
      for (int i = key.length() - 1; i >= 0; i--) {
        if (i < 3) {
          tempBits1 |= key.charAt(i);
          tempBits1 <<= 16;
        } else {
          tempBits2 <<= 16;
          tempBits2 |= key.charAt(i);
        }
      }
    }
    tempBits1 |= key.length();
    this.bits1 = tempBits1;
    this.bits2 = tempBits2;

  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMStatsDiskRegionEntryHeapStringKey2> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapStringKey2.class, "lastModified");

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

  // stats code

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
    setLastModified(lastModified);
    if (!DISABLE_ACCESS_TIME_UPDATE_ON_PUT) {
      setLastAccessed(lastAccessed);
    }
  }

  private volatile long lastAccessed;
  private volatile int hitCount;
  private volatile int missCount;

  private static final AtomicIntegerFieldUpdater<VMStatsDiskRegionEntryHeapStringKey2> hitCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapStringKey2.class, "hitCount");

  private static final AtomicIntegerFieldUpdater<VMStatsDiskRegionEntryHeapStringKey2> missCountUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VMStatsDiskRegionEntryHeapStringKey2.class, "missCount");

  @Override
  public long getLastAccessed() throws InternalStatisticsDisabledException {
    return this.lastAccessed;
  }

  private void setLastAccessed(final long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  @Override
  public long getHitCount() throws InternalStatisticsDisabledException {
    return this.hitCount & 0xFFFFFFFFL;
  }

  @Override
  public long getMissCount() throws InternalStatisticsDisabledException {
    return this.missCount & 0xFFFFFFFFL;
  }

  private void incrementHitCount() {
    hitCountUpdater.incrementAndGet(this);
  }

  private void incrementMissCount() {
    missCountUpdater.incrementAndGet(this);
  }

  @Override
  public void resetCounts() throws InternalStatisticsDisabledException {
    hitCountUpdater.set(this, 0);
    missCountUpdater.set(this, 0);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  @Override
  public void txDidDestroy(long timeStamp) {
    setLastModified(timeStamp);
    setLastAccessed(timeStamp);
    this.hitCount = 0;
    this.missCount = 0;
  }

  @Override
  public boolean hasStats() {
    return true;
  }



  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  // key code


  /**
   * strlen is encoded in lowest 6 bits (max strlen is 63)<br>
   * character encoding info is in bits 7 and 8<br>
   * The other bits are used to encoded character data.
   */
  private final long bits1;

  /**
   * bits2 encodes character data
   */
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
  public Object getKey() {
    int keyLength = getKeyLength();
    char[] chars = new char[keyLength];
    long tempBits1 = this.bits1;
    long tempBits2 = this.bits2;

    if (getEncoding() == 1) {
      for (int i = 0; i < keyLength; i++) {
        if (i < 7) {
          tempBits1 >>= 8;
          chars[i] = (char) (tempBits1 & 0x00ff);
        } else {
          chars[i] = (char) (tempBits2 & 0x00ff);
          tempBits2 >>= 8;
        }
      }

    } else {
      for (int i = 0; i < keyLength; i++) {
        if (i < 3) {
          tempBits1 >>= 16;
          chars[i] = (char) (tempBits1 & 0x00FFff);
        } else {
          chars[i] = (char) (tempBits2 & 0x00FFff);
          tempBits2 >>= 16;
        }
      }
    }
    return new String(chars);
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp

  @Override
  public boolean isKeyEqual(final Object key) {
    if (key instanceof String) {
      String stringKey = (String) key;
      int keyLength = getKeyLength();
      if (stringKey.length() == keyLength) {
        long tempBits1 = this.bits1;
        long tempBits2 = this.bits2;

        if (getEncoding() == 1) {
          for (int i = 0; i < keyLength; i++) {
            char character;
            if (i < 7) {
              tempBits1 >>= 8;
              character = (char) (tempBits1 & 0x00ff);
            } else {
              character = (char) (tempBits2 & 0x00ff);
              tempBits2 >>= 8;
            }
            if (stringKey.charAt(i) != character) {
              return false;
            }
          }

        } else {
          for (int i = 0; i < keyLength; i++) {
            char character;
            if (i < 3) {
              tempBits1 >>= 16;
              character = (char) (tempBits1 & 0x00FFff);
            } else {
              character = (char) (tempBits2 & 0x00FFff);
              tempBits2 >>= 16;
            }
            if (stringKey.charAt(i) != character) {
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

