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
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer;

/**
 * VersionTags are sent with distribution messages and carry version info
 * for the operation.
 * <p/>
 * Note that on the receiving end the membership IDs in a version tag will
 * not be references to canonical IDs and should be made so before storing
 * them for any length of time.
 * <p/>
 * This class implements java.io.Serializable for dunit testing.  It should
 * not otherwise be serialized with that mechanism.
 *
 */
public abstract class VersionTag<T extends VersionSource> implements DataSerializableFixedID, java.io.Serializable, VersionHolder<T> {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 9098338414308465271L;

  // tag_size represents the tag, but does not count member ID sizes since they are
  // interned in the region version vectors
  public static final int TAG_SIZE = ReflectionSingleObjectSizer.OBJECT_SIZE +
          ReflectionSingleObjectSizer.REFERENCE_SIZE * 2 + 23;

  /**
   * A timestamp that cannot exist due to range restrictions.  This is used
   * to mark a timestamp as not being real
   */
  public static final long ILLEGAL_VERSION_TIMESTAMP = 0x8000000000000000l;


  // flags for serialization
  private static final int HAS_MEMBER_ID = 0x01;
  private static final int HAS_PREVIOUS_MEMBER_ID = 0x02;
  private static final int VERSION_TWO_BYTES = 0x04;
  private static final int DUPLICATE_MEMBER_IDS = 0x08;
  private static final int HAS_RVV_HIGH_BYTE = 0x10;

  private static final int BITS_POSDUP = 0x01;
  private static final int BITS_RECORDED = 0x02; // has the rvv recorded this?
  private static final int BITS_HAS_PREVIOUS_ID = 0x04;
  private static final int BITS_GATEWAY_TAG = 0x08;
  private static final int BITS_IS_REMOTE_TAG = 0x10;
  private static final int BITS_TIMESTAMP_APPLIED = 0x20;

  private static final int BITS_ALLOWED_BY_RESOLVER = 0x40;
  // Note: the only valid BITS_* are 0xFFFF.
  
  /**
   * the per-entry version number for the operation
   */
  private int entryVersion;

  /**
   * high byte for large region version numbers
   */
  private short regionVersionHighBytes;

  /**
   * low bytes for region version numbers
   */
  private int regionVersionLowBytes;

  /**
   * time stamp
   */
  private long timeStamp;

  /**
   * distributed system ID
   */
  private byte distributedSystemId;

  // In GEODE-1252 we found that the bits field
  // was concurrently modified by calls to
  // setPreviousMemberID and setRecorded.
  // So bits has been changed to volatile and
  // all modification to it happens using AtomicIntegerFieldUpdater.
  private static final AtomicIntegerFieldUpdater<VersionTag> bitsUpdater =
      AtomicIntegerFieldUpdater.newUpdater(VersionTag.class, "bits");
  /**
   * boolean bits
   * Note: this is an int field so it has 32 bits BUT only the lower 16 bits are serialized.
   * So all our code should treat this an an unsigned short field.
   */
  private volatile int bits;

  /**
   * the initiator of the operation.  If null, the initiator was the sender
   * of the operation
   */
  private T memberID;

  /**
   * for Delta operations, the ID of the version stamp on which the delta
   * is based.  The version number for that stamp is getEntryVersion()-1
   */
  private T previousMemberID;

  public boolean isFromOtherMember() {
    return (this.bits & BITS_IS_REMOTE_TAG) != 0;
  }
  
  /** was the timestamp of this tag used to update the cache's timestamp? */
  public boolean isTimeStampUpdated() {
    return (this.bits & BITS_TIMESTAMP_APPLIED) != 0;
  }

  /** record that the timestamp from this tag was applied to the cache */
  public void setTimeStampApplied(boolean isTimeStampUpdated) {
    if (isTimeStampUpdated) {
      setBits(BITS_TIMESTAMP_APPLIED);
    } else {
      clearBits(~BITS_TIMESTAMP_APPLIED);
    }
  }

  /**
   * @return true if this is a gateway timestamp holder rather than a full version tag
   */
  public boolean isGatewayTag() {
    return (this.bits & BITS_GATEWAY_TAG) != 0;
  }

  public void setEntryVersion(int version) {
    this.entryVersion = version;
  }

  public int getEntryVersion() {
    return this.entryVersion;
  }

  public void setVersionTimeStamp(long timems) {
    this.timeStamp = timems;
  }

  public void setIsGatewayTag(boolean isGateway) {
    if (isGateway) {
      setBits(BITS_GATEWAY_TAG);
    } else {
      clearBits(~BITS_GATEWAY_TAG);
    }
  }

  public void setRegionVersion(long version) {
    this.regionVersionHighBytes = (short) ((version & 0xFFFF00000000L) >> 32);
    this.regionVersionLowBytes = (int) (version & 0xFFFFFFFFL);
  }

  public long getRegionVersion() {
    return (((long)regionVersionHighBytes) << 32) | (regionVersionLowBytes & 0x00000000FFFFFFFFL);  
  }

  /**
   * set rvv internal bytes.  Used by region entries
   */
  public void setRegionVersion(short highBytes, int lowBytes) {
    this.regionVersionHighBytes = highBytes;
    this.regionVersionLowBytes = lowBytes;
  }

  /**
   * get rvv internal high byte.  Used by region entries for transferring to storage
   */
  public short getRegionVersionHighBytes() {
    return this.regionVersionHighBytes;
  }

  /**
   * get rvv internal low bytes.  Used by region entries for transferring to storage
   */
  public int getRegionVersionLowBytes() {
    return this.regionVersionLowBytes;
  }

  /**
   * set that this tag has been recorded in a receiver's RVV
   */
  public void setRecorded() {
    setBits(BITS_RECORDED);
  }

  /**
   * has this tag been recorded in a receiver's RVV?
   */
  public boolean isRecorded() {
    return ((this.bits & BITS_RECORDED) != 0);
  }
  
  /**
   * Set canonical ID objects into this version tag using the DM's cache
   * of IDs
   * @param distributionManager
   */
  public void setCanonicalIDs(DM distributionManager) {
  }

  /**
   * @return the memberID
   */
  public T getMemberID() {
    return this.memberID;
  }

  /**
   * @param memberID the memberID to set
   */
  public void setMemberID(T memberID) {
    this.memberID = memberID;
  }

  /**
   * @return the previousMemberID
   */
  public T getPreviousMemberID() {
    return this.previousMemberID;
  }

  /**
   * @param previousMemberID the previousMemberID to set
   */
  public void setPreviousMemberID(T previousMemberID) {
    setBits(BITS_HAS_PREVIOUS_ID);
    this.previousMemberID = previousMemberID;
  }

  /**
   * sets the possible-duplicate flag for this tag.  When a tag has this
   * bit it means that the cache had seen the operation that was being applied
   * to it and plucked out the current version stamp to use in propagating
   * the event to other members and clients.  A member receiving this event
   * should not allow duplicate application of the event to the cache.
   */
  public VersionTag setPosDup(boolean flag) {
    if (flag) {
      setBits(BITS_POSDUP);
    } else {
      clearBits(~BITS_POSDUP);
    }
    return this;
  }

  public boolean isPosDup() {
    return (this.bits & BITS_POSDUP) != 0;
  }

  /**
   * set or clear the flag that this tag was blessed by a
   * conflict resolver
   * @param flag
   * @return this tag
   */
  public VersionTag setAllowedByResolver(boolean flag) {
    if (flag) {
      setBits(BITS_ALLOWED_BY_RESOLVER);
    } else {
      clearBits(~BITS_ALLOWED_BY_RESOLVER);
    }
    return this;
  }
  
  public boolean isAllowedByResolver() {
    return (this.bits & BITS_ALLOWED_BY_RESOLVER) != 0;
  }
  
  public int getDistributedSystemId() {
    return this.distributedSystemId;
  }

  public void setDistributedSystemId(int id) {
    this.distributedSystemId = (byte) (id & 0xFF);
  }

  /**
   * replace null member IDs with the given identifier.  This is used to
   * incorporate version information into the cache that has been received
   * from another VM
   *
   * @param id
   */
  public void replaceNullIDs(VersionSource id) {
    if (this.memberID == null) {
      this.memberID = (T) id;
    }
    if (this.previousMemberID == null && this.hasPreviousMemberID() && entryVersion > 1) {
      this.previousMemberID = (T) id;
    }
  }

  /**
   * returns true if this tag has a previous member ID for delta operation
   * checks
   */
  public boolean hasPreviousMemberID() {
    return (this.bits & BITS_HAS_PREVIOUS_ID) != 0;
  }

  /**
   * returns true if entry and region version numbers are not both zero, meaning this
   * has valid version numbers
   */
  public boolean hasValidVersion() {
    return !(this.entryVersion == 0 && this.regionVersionHighBytes == 0 && this.regionVersionLowBytes == 0);
  }

  public void toData(DataOutput out) throws IOException {
    toData(out, true);
  }

  public void toData(DataOutput out, boolean includeMember) throws IOException {
    int flags = 0;
    boolean versionIsShort = false;
    if (this.entryVersion < 0x10000) {
      versionIsShort = true;
      flags |= VERSION_TWO_BYTES;
    }
    if (this.regionVersionHighBytes != 0) {
      flags |= HAS_RVV_HIGH_BYTE;
    }
    if (this.memberID != null && includeMember) {
      flags |= HAS_MEMBER_ID;
    }
    if (this.previousMemberID != null) {
      flags |= HAS_PREVIOUS_MEMBER_ID;
      if (this.previousMemberID == this.memberID && includeMember) {
        flags |= DUPLICATE_MEMBER_IDS;
      }
    }
    if (logger.isTraceEnabled(LogMarker.VERSION_TAG)) {
      logger.info(LogMarker.VERSION_TAG, "serializing {} with flags 0x{}", this.getClass(), Integer.toHexString(flags));
    }
    out.writeShort(flags);
    out.writeShort(this.bits);
    out.write(this.distributedSystemId);
    if (versionIsShort) {
      out.writeShort(this.entryVersion & 0xffff);
    } else {
      out.writeInt(this.entryVersion);
    }
    if (this.regionVersionHighBytes != 0) {
      out.writeShort(this.regionVersionHighBytes);
    }
    out.writeInt(this.regionVersionLowBytes);
    InternalDataSerializer.writeUnsignedVL(this.timeStamp, out);
    if (this.memberID != null && includeMember) {
      writeMember(this.memberID, out);
    }
    if (this.previousMemberID != null && (this.previousMemberID != this.memberID || !includeMember)) {
      writeMember(this.previousMemberID, out);
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int flags = in.readUnsignedShort();
    if (logger.isTraceEnabled(LogMarker.VERSION_TAG)) {
      logger.info(LogMarker.VERSION_TAG, "deserializing {} with flags 0x{}", this.getClass(), Integer.toHexString(flags));
    }
    bitsUpdater.set(this, in.readUnsignedShort());
    this.distributedSystemId = in.readByte();
    if ((flags & VERSION_TWO_BYTES) != 0) {
      this.entryVersion = in.readShort() & 0xffff;
    } else {
      this.entryVersion = in.readInt() & 0xffffffff;
    }
    if ((flags & HAS_RVV_HIGH_BYTE) != 0) {
      this.regionVersionHighBytes = in.readShort();
    }
    this.regionVersionLowBytes = in.readInt();
    this.timeStamp = InternalDataSerializer.readUnsignedVL(in);
    if ((flags & HAS_MEMBER_ID) != 0) {
      this.memberID = readMember(in);
    }
    if ((flags & HAS_PREVIOUS_MEMBER_ID) != 0) {
      if ((flags & DUPLICATE_MEMBER_IDS) != 0) {
        this.previousMemberID = this.memberID;
      } else {
        this.previousMemberID = readMember(in);
      }
    }
    setIsRemoteForTesting();
  }
  
  public void setIsRemoteForTesting() {
    setBits(BITS_IS_REMOTE_TAG);
  }

  public abstract T readMember(DataInput in) throws IOException, ClassNotFoundException;

  public abstract void writeMember(T memberID, DataOutput out) throws IOException;


  public int getSizeInBytes() {
    int size = com.gemstone.gemfire.internal.cache.lru.Sizeable.PER_OBJECT_OVERHEAD + VersionTag.TAG_SIZE;
    // member size calculation 
    size += memberID.getSizeInBytes();
    return size;
    
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    if (isGatewayTag()) {
      s.append("{ds=").append(this.distributedSystemId)
              .append("; time=").append(getVersionTimeStamp()).append("}");
    } else {
      s.append("{v").append(this.entryVersion);
      s.append("; rv").append(getRegionVersion());
      if (this.memberID != null) {
        s.append("; mbr=").append(this.memberID);
      }
      if (hasPreviousMemberID()) {
        s.append("; prev=").append(this.previousMemberID);
      }
      if (this.distributedSystemId >= 0) {
        s.append("; ds=").append(this.distributedSystemId);
      }
      s.append("; time=").append(getVersionTimeStamp());
      if (isFromOtherMember()) {
        s.append("; remote");
      }
      if (this.isAllowedByResolver()) {
        s.append("; allowed");
      }
      s.append("}");
    }
    return s.toString();
  }


  /**
   * @return the time stamp of this operation.  This is an unsigned integer returned as a long
   */
  public long getVersionTimeStamp() {
    return this.timeStamp;
  }

  /**
   * Creates a version tag of the appropriate type, based on the member id
   *
   * @param memberId
   */
  public static VersionTag create(VersionSource memberId) {
    VersionTag tag;
    if (memberId instanceof DiskStoreID) {
      tag = new DiskVersionTag();
    } else {
      tag = new VMVersionTag();
    }

    tag.setMemberID(memberId);

    return tag;
  }

  public static VersionTag create(boolean persistent, DataInput in)
          throws IOException, ClassNotFoundException {
    VersionTag<?> tag;
    if (persistent) {
      tag = new DiskVersionTag();
    } else {
      tag = new VMVersionTag();
    }
    InternalDataSerializer.invokeFromData(tag, in);
    return tag;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + entryVersion;
    result = prime * result + ((memberID == null) ? 0 : memberID.hashCode());
    result = prime * result + regionVersionHighBytes;
    result = prime * result + regionVersionLowBytes;
    if (isGatewayTag()) {
      result = prime * result + (int) timeStamp;
      result = prime * result + (int) (timeStamp >>> 32);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VersionTag<?> other = (VersionTag<?>) obj;
    if (entryVersion != other.entryVersion)
      return false;
    if (memberID == null) {
      if (other.memberID != null)
        return false;
    } else if (!memberID.equals(other.memberID))
      return false;
    if (regionVersionHighBytes != other.regionVersionHighBytes)
      return false;
    if (regionVersionLowBytes != other.regionVersionLowBytes)
      return false;
    if (isGatewayTag() != other.isGatewayTag()) {
      return false;
    }
    if (isGatewayTag()) {
      if (timeStamp != other.timeStamp) {
        return false;
      }
      if (distributedSystemId != other.distributedSystemId) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Set any bits in the given bitMask on the bits field
   */
  private void setBits(int bitMask) {
    int oldBits;
    int newBits;
    do {
      oldBits = this.bits;
      newBits = oldBits | bitMask;
    } while (!bitsUpdater.compareAndSet(this, oldBits, newBits));
  }
  /**
   * Clear any bits not in the given bitMask from the bits field
   */
  private void clearBits(int bitMask) {
    int oldBits;
    int newBits;
    do {
      oldBits = this.bits;
      newBits = oldBits & bitMask;
    } while (!bitsUpdater.compareAndSet(this, oldBits, newBits));
  }
}
