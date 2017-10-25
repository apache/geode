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

import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * This id stores seven pieces of information:
 * <ul>
 * <li>The unique identifier which will identify this entry called the keyId
 * <li>The oplog id identifying the oplog in which this entry's value is present
 * <li>The position in the oplog (the oplog offset) where this entry's value is stored
 * <li>The length of the byte array (which represent the value)on disk
 * <li>Userbits of the value
 * </ul>
 * 
 * @since GemFire 5.1
 */

public abstract class DiskId {

  // @todo this field could be an int for an overflow only region
  /**
   * id consists of most significant 1 byte = users bits 2-8 bytes = oplog id least significant.
   * 
   * The highest bit in the oplog id part is set to 1 if the oplog id is negative.
   */
  private long id;

  /**
   * Length of the bytes on disk. This is always set. If the value is invalid then it will be set to
   * 0. The most significant bit is used by overflow to mark it as needing to be written.
   */
  protected int valueLength = 0;

  /** bit position in id for setting flush buffer toggle bit * */
  // private static final int IS_FLUSH_BUFFER_TOGGLE_BIT = 0x20 << 24;

  // userBits of the value on disk
  // private byte userBits = -1;

  public abstract long getKeyId();

  /** Returns the offset in oplog where the entry is stored */
  public abstract long getOffsetInOplog();


  /**
   * Bit masks to extract the oplog id or user bits from the id field The oplog id is currently the
   * lowest 7 bytes, and the user bits is the most significant byte. The sign of the oplog id is
   * held in the highest bit of the oplog id bytes.
   */
  public static final long OPLOG_ID_MASK = 0x00FFFFFFFFFFFFFFL;
  public static final long USER_BITS_MASK = 0xFF00000000000000L;
  public static final long MAX_OPLOG_ID = 0x007FFFFFFFFFFFFFL;
  public static final long OPLOG_ID_SIGN_BIT = 0x0080000000000000L;
  public static final long USER_BITS_SHIFT = 24 + 32;

  /**
   * @return Returns the oplog id.
   */
  public synchronized long getOplogId() {
    // mask the first byte to get the oplogId
    long oplogId = this.id & MAX_OPLOG_ID;

    // Check to see if the oplog id should be negative
    if ((this.id & OPLOG_ID_SIGN_BIT) != 0) {
      oplogId = -1L * oplogId;
    }

    return oplogId;
  }

  public abstract void setKeyId(long keyId);

  /**
   * Setter for oplog offset of an entry
   * 
   * @param offsetInOplog - offset in oplog where the entry is stored.
   */
  public abstract void setOffsetInOplog(long offsetInOplog);

  public abstract void markForWriting();

  public abstract void unmarkForWriting();

  public abstract boolean needsToBeWritten();

  /**
   * Returns previous oplog id
   */
  public synchronized long setOplogId(long oplogId) {
    long result = getOplogId();
    long oldUserBits = this.id & USER_BITS_MASK;// only get the most significant byte containing
    // sign bit + toggle flag + user bits
    long opId = oplogId;
    if (oplogId < 0) {
      opId = -1 * oplogId;// make oplogId positive
      opId |= OPLOG_ID_SIGN_BIT; // Set the highest bit of the oplog id to be
      // 1 to indicate a negative number
    }
    this.id = opId | oldUserBits;

    // Assert.assertTrue(oplogId == getOplogId());
    return result;
  }

  /**
   * @return Returns the userBits.
   */
  public synchronized byte getUserBits() {
    return (byte) (this.id >> USER_BITS_SHIFT); // shift to right to get the user bits
  }

  /**
   * @param userBits The userBit to set.
   */
  public synchronized void setUserBits(byte userBits) {
    long userLong = ((long) userBits) << USER_BITS_SHIFT;// set it as most signifcant byte.

    this.id &= OPLOG_ID_MASK; // mask the most significant byte in id.
    this.id |= userLong; // set the most significant byte in id.

    // Assert.assertTrue(userBit == getUserBits());
  }

  /**
   * Return true if entry is schedule to be async written to disk. Return false if it has already
   * been written or was never modified.
   * 
   * @since GemFire prPersistSprint1
   */
  public boolean isPendingAsync() {
    return EntryBits.isPendingAsync(getUserBits());
  }

  /**
   * @since GemFire prPersistSprint1
   */
  public synchronized void setPendingAsync(boolean v) {
    byte origBits = getUserBits();
    byte newBits = EntryBits.setPendingAsync(origBits, v);
    if (origBits != newBits) {
      setUserBits(newBits);
    }
  }

  public synchronized void setRecoveredFromDisk(boolean v) {
    byte origBits = getUserBits();
    byte newBits = EntryBits.setRecoveredFromDisk(origBits, v);
    if (origBits != newBits) {
      setUserBits(newBits);
    }
  }

  /**
   * @return Returns the valueLength.
   */
  public int getValueLength() {
    return valueLength & 0x7fffffff;
  }

  /**
   * @param valueLength The valueLength to set.
   */
  public void setValueLength(int valueLength) {
    if (valueLength < 0) {
      throw new IllegalStateException(
          "Expected DiskId valueLength " + valueLength + " to be >= 0.");
    }
    this.valueLength = (this.valueLength & (0x80000000)) | valueLength;
  }

  public DiskEntry getPrev() {
    return null;
  }

  public DiskEntry getNext() {
    return null;
  }

  public void setPrev(DiskEntry v) {
    throw new IllegalStateException("should only be called by disk compaction");
  }

  public void setNext(DiskEntry v) {
    throw new IllegalStateException("should only be called by disk compaction");
  }

  @Override
  public String toString() {
    /*
     * StringBuffer temp = new StringBuffer("Oplog Key ID = "); temp.append(this.keyId);
     */
    StringBuilder temp = new StringBuilder("Oplog ID = ");
    temp.append(this.getOplogId());
    temp.append("; Offset in Oplog = ");
    temp.append(getOffsetInOplog());
    temp.append("; Value Length = ");
    temp.append(getValueLength());
    temp.append("; UserBits is = ");
    temp.append(this.getUserBits());
    return temp.toString();
  }

  /**
   * Creates appropriate instance of DiskId depending upon the maxOplogSize set by the user. If the
   * maxOplogSize (in bytes) is greater than Integer.MAX_VALUE, LongOplogOffsetDiskId will be
   * created and for maxOplogSize lesser than that, IntOplogOffsetDiskId will be created.
   * 
   * @return the disk-id instance created.
   */
  public static DiskId createDiskId(long maxOplogSize, boolean isPersistenceType,
      boolean needsLinkedList) {
    long bytes = maxOplogSize * 1024 * 1024;
    if (bytes > Integer.MAX_VALUE) {
      if (isPersistenceType) {
        if (needsLinkedList) {
          return new PersistenceWithLongOffset();
        } else {
          return new PersistenceWithLongOffsetNoLL();
        }
      } else {
        if (needsLinkedList) {
          return new OverflowOnlyWithLongOffset();
        } else {
          return new OverflowOnlyWithLongOffsetNoLL();
        }
      }
    } else {
      if (isPersistenceType) {
        if (needsLinkedList) {
          return new PersistenceWithIntOffset();
        } else {
          return new PersistenceWithIntOffsetNoLL();
        }
      } else {
        if (needsLinkedList) {
          return new OverflowOnlyWithIntOffset();
        } else {
          return new OverflowOnlyWithIntOffsetNoLL();
        }
      }
    }
  }

  /**
   * Test method to verify if the passed DiskId is an instance of PersistenceWithIntOffset.
   * 
   * @param diskId - the DiskId instance
   * @return true if the given DiskId is an instance of PersistenceWithIntOffset
   */
  static boolean isInstanceofPersistIntOplogOffsetDiskId(DiskId diskId) {
    return diskId instanceof PersistenceWithIntOffset;
  }

  /**
   * Test method to verify if the passed DiskId is an instance of PersistenceWithLongOffset.
   * 
   * @param diskId - the DiskId instance
   * @return true if the given DiskId is an instance of PersistenceWithLongOffset
   */
  static boolean isInstanceofPersistLongOplogOffsetDiskId(DiskId diskId) {
    return diskId instanceof PersistenceWithLongOffset;
  }

  /**
   * Test method to verify if the passed DiskId is an instance of OverflowOnlyWithIntOffset.
   * 
   * @param diskId - the DiskId instance
   * @return true if the given DiskId is an instance of OverflowOnlyWithIntOffset
   */
  static boolean isInstanceofOverflowIntOplogOffsetDiskId(DiskId diskId) {
    return diskId instanceof OverflowOnlyWithIntOffset;
  }

  /**
   * Test method to verify if the passed DiskId is an instance of PersistenceWithLongOffset.
   * 
   * @param diskId - the DiskId instance
   * @return true if the given DiskId is an instance of LongOplogOffsetDiskId
   */
  static boolean isInstanceofOverflowOnlyWithLongOffset(DiskId diskId) {
    return diskId instanceof OverflowOnlyWithLongOffset;
  }

  /**
   * Inner class implementation of DiskId which stores offset in oplog as 'int' field.
   * 
   * 
   */
  protected abstract static class IntOplogOffsetDiskId extends DiskId {
    /**
     * The position in the oplog (the oplog offset) where this entry's value is stored
     */
    private volatile int offsetInOplog;

    /**
     * @return the offset in oplog where the entry is stored (returned as long)
     */
    @Override
    public long getOffsetInOplog() {
      return offsetInOplog;
    }

    /**
     * Setter for oplog offset of an entry
     * 
     * @param offsetInOplog - offset in oplog where the entry is stored.
     */
    @Override
    public void setOffsetInOplog(long offsetInOplog) {
      this.offsetInOplog = (int) offsetInOplog;
    }
  }

  /**
   * Inner class implementation of DiskId which stores offset in oplog as 'long' field.
   * 
   * 
   */
  protected abstract static class LongOplogOffsetDiskId extends DiskId {
    /**
     * The position in the oplog (the oplog offset) where this entry's value is stored
     */
    private volatile long offsetInOplog;

    /**
     * @return the offset in oplog where the entry is stored.
     */
    @Override
    public long getOffsetInOplog() {
      return offsetInOplog;
    }

    /**
     * Setter for oplog offset of an entry
     * 
     * @param offsetInOplog - offset in oplog where the entry is stored.
     */
    @Override
    public void setOffsetInOplog(long offsetInOplog) {
      this.offsetInOplog = offsetInOplog;
    }
  }

  protected static class OverflowOnlyWithIntOffsetNoLL extends IntOplogOffsetDiskId {
    OverflowOnlyWithIntOffsetNoLL() {
      markForWriting();
    }

    @Override
    public long getKeyId() {
      throw new UnsupportedOperationException(
          LocalizedStrings.DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_QUERIED
              .toLocalizedString());
    }

    @Override
    public void setKeyId(long keyId) {
      throw new UnsupportedOperationException(
          LocalizedStrings.DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_SET
              .toLocalizedString());
    }

    @Override
    public void markForWriting() {
      this.valueLength |= 0x80000000;
    }

    @Override
    public void unmarkForWriting() {
      this.valueLength &= 0x7fffffff;
    }

    @Override
    public boolean needsToBeWritten() {
      return (this.valueLength & 0x80000000) != 0;
    }
  }
  protected static final class OverflowOnlyWithIntOffset extends OverflowOnlyWithIntOffsetNoLL {
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry prev;
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry next;

    @Override
    public DiskEntry getPrev() {
      return this.prev;
    }

    @Override
    public DiskEntry getNext() {
      return this.next;
    }

    @Override
    public void setPrev(DiskEntry v) {
      this.prev = v;
    }

    @Override
    public void setNext(DiskEntry v) {
      this.next = v;
    }
  }

  protected static class OverflowOnlyWithLongOffsetNoLL extends LongOplogOffsetDiskId {
    OverflowOnlyWithLongOffsetNoLL() {
      markForWriting();
    }

    @Override
    public long getKeyId() {
      throw new UnsupportedOperationException(
          LocalizedStrings.DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_QUERIED
              .toLocalizedString());
    }

    @Override
    public void setKeyId(long keyId) {
      throw new UnsupportedOperationException(
          LocalizedStrings.DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_SET
              .toLocalizedString());
    }

    @Override
    public void markForWriting() {
      this.valueLength |= 0x80000000;
    }

    @Override
    public void unmarkForWriting() {
      this.valueLength &= 0x7fffffff;
    }

    @Override
    public boolean needsToBeWritten() {
      return (this.valueLength & 0x80000000) != 0;
    }
  }
  protected static final class OverflowOnlyWithLongOffset extends OverflowOnlyWithLongOffsetNoLL {
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry prev;
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry next;

    @Override
    public DiskEntry getPrev() {
      return this.prev;
    }

    @Override
    public DiskEntry getNext() {
      return this.next;
    }

    @Override
    public void setPrev(DiskEntry v) {
      this.prev = v;
    }

    @Override
    public void setNext(DiskEntry v) {
      this.next = v;
    }
  }

  protected static class PersistenceWithIntOffsetNoLL extends IntOplogOffsetDiskId {
    /** unique entry identifier * */
    private long keyId;

    @Override
    public long getKeyId() {
      return keyId;
    }

    @Override
    public void setKeyId(long keyId) {
      this.keyId = keyId;
    }

    @Override
    public void markForWriting() {
      throw new IllegalStateException("Should not be used for persistent region");
    }

    @Override
    public void unmarkForWriting() {
      // Do nothing
    }

    @Override
    public boolean needsToBeWritten() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder temp = new StringBuilder("Oplog Key ID = ");
      temp.append(this.keyId);
      temp.append("; ");
      temp.append(super.toString());
      return temp.toString();
    }
  }
  protected static final class PersistenceWithIntOffset extends PersistenceWithIntOffsetNoLL {
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry prev;
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry next;

    @Override
    public DiskEntry getPrev() {
      return this.prev;
    }

    @Override
    public DiskEntry getNext() {
      return this.next;
    }

    @Override
    public void setPrev(DiskEntry v) {
      this.prev = v;
    }

    @Override
    public void setNext(DiskEntry v) {
      this.next = v;
    }
  }

  protected static class PersistenceWithLongOffsetNoLL extends LongOplogOffsetDiskId {
    /** unique entry identifier * */
    private long keyId;

    @Override
    public long getKeyId() {
      return keyId;
    }

    @Override
    public void setKeyId(long keyId) {
      this.keyId = keyId;
    }

    @Override
    public void markForWriting() {
      throw new IllegalStateException("Should not be used for persistent region");
    }

    @Override
    public void unmarkForWriting() {
      // Do nothing
    }

    @Override
    public String toString() {
      StringBuilder temp = new StringBuilder("Oplog Key ID = ");
      temp.append(this.keyId);
      temp.append("; ");
      temp.append(super.toString());
      return temp.toString();

    }

    @Override
    public boolean needsToBeWritten() {
      return false;
    }
  }
  protected static final class PersistenceWithLongOffset extends PersistenceWithLongOffsetNoLL {
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry prev;
    /**
     * Used by DiskRegion for compaction
     * 
     * @since GemFire prPersistSprint1
     */
    private DiskEntry next;

    @Override
    public DiskEntry getPrev() {
      return this.prev;
    }

    @Override
    public DiskEntry getNext() {
      return this.next;
    }

    @Override
    public void setPrev(DiskEntry v) {
      this.prev = v;
    }

    @Override
    public void setNext(DiskEntry v) {
      this.next = v;
    }
  }
}

