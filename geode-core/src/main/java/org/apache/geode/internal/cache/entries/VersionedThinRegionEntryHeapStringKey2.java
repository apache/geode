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
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
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
public class VersionedThinRegionEntryHeapStringKey2 extends VersionedThinRegionEntryHeap {
  // --------------------------------------- common fields ----------------------------------------
  private static final AtomicLongFieldUpdater<VersionedThinRegionEntryHeapStringKey2> LAST_MODIFIED_UPDATER =
      AtomicLongFieldUpdater.newUpdater(VersionedThinRegionEntryHeapStringKey2.class,
          "lastModified");
  protected int hash;
  private HashEntry<Object, Object> nextEntry;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private volatile Object value;
  // ------------------------------------- versioned fields ---------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private VersionSource memberId;
  private short entryVersionLowBytes;
  private short regionVersionHighBytes;
  private int regionVersionLowBytes;
  private byte entryVersionHighByte;
  private byte distributedSystemId;
  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
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

  public VersionedThinRegionEntryHeapStringKey2(final RegionEntryContext context, final String key,
      final Object value, final boolean byteEncode) {
    super(context, value);
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
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
  @Override
  protected Object getValueField() {
    return this.value;
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
    return this.hash;
  }

  @Override
  protected void setEntryHash(final int hash) {
    this.hash = hash;
  }

  @Override
  public HashEntry<Object, Object> getNextEntry() {
    return this.nextEntry;
  }

  @Override
  public void setNextEntry(final HashEntry<Object, Object> nextEntry) {
    this.nextEntry = nextEntry;
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

  // ----------------------------------------- key code -------------------------------------------
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
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
