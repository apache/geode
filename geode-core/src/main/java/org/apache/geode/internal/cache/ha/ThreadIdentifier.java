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
package org.apache.geode.internal.cache.ha;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

/**
 * Class identifying a Thread uniquely across the distributed system. It is composed of two fields
 * 1) A byte array uniquely identifying the distributed system 2) A long value unqiuely identifying
 * the thread in the distributed system
 *
 * The application thread while operating on the Region gets an EventID object ( contained in
 * EntryEventImpl) This EventID object contains a ThreadLocal field which uniquely identifies the
 * thread by storing the Object of this class.
 *
 * @see EventID
 *
 *
 */

public class ThreadIdentifier implements DataSerializable {
  private static final long serialVersionUID = 3366884860834823186L;

  private byte[] membershipID;

  private long threadID;

  public static final long MAX_THREAD_PER_CLIENT = 1000000L;
  public static final int MAX_BUCKET_PER_PR = 1000;
  public static final long WAN_BITS_MASK = 0xFFFFFFFF00000000L;

  /**
   * Generates thread ids for parallel wan usage.
   */
  public enum WanType {
    RESERVED, // original thread id incl putAll (or old format)
    PRIMARY, // parallel new wan
    SECONDARY, // parallel new wan
    PARALLEL; // parallel old wan

    /**
     * Generates a new thread id for usage in a parallel wan context.
     *
     * @param threadId the original thread id
     * @param offset the thread offset
     * @param gatewayIndex the index of the gateway
     * @return the new thread id
     */
    public long generateWanId(long threadId, long offset, int gatewayIndex) {
      assert this != RESERVED;
      return Bits.WAN_TYPE.shift(ordinal()) | Bits.WAN.shift(offset)
          | Bits.GATEWAY_ID.shift(gatewayIndex) | threadId;
    }

    /**
     * Returns true if the supplied value is a wan thread identifier.
     *
     * @param tid the thread
     * @return true if the thread id is one of the wan types
     */
    public static boolean matches(long tid) {
      return Bits.WAN_TYPE.extract(tid) > 0;
    }
  }

  /**
   * Provides type-safe bitwise access to the threadID when dealing with generated values for wan id
   * generation.
   */
  public enum Bits {
    THREAD_ID(0, 32), // bits 0-31 thread id (including fake putAll bits)
    WAN(32, 16), // bits 32-47 wan thread index (or bucket for new wan)
    WAN_TYPE(48, 8), // bits 48-55 thread id type
    GATEWAY_ID(56, 7), // bits 56-62 gateway id (bit 63 would make the thread id negative)
    RESERVED(63, 1); // bit 63 unused

    /** the beginning bit position */
    private final int position;

    /** the field width */
    private final int width;

    private Bits(int position, int width) {
      this.position = position;
      this.width = width;
    }

    /**
     * Returns the field bitmask.
     *
     * @return the mask
     */
    public long mask() {
      return (1L << width) - 1;
    }

    /**
     * Returns the value shifted into the field position.
     *
     * @param val the value to shift
     * @return the shifted value
     */
    public long shift(long val) {
      assert val <= mask() : "Input value " + val + " is too large for " + this
          + " which has a maximum of " + mask();
      return val << position;
    }

    /**
     * Extracts the field bits from the value.
     *
     * @param val the value
     * @return the field
     */
    public long extract(long val) {
      return (val >> position) & mask();
    }
  }

  public ThreadIdentifier() {}

  public ThreadIdentifier(final byte[] mid, long threadId) {
    this.membershipID = mid;
    this.threadID = threadId;
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj == null) || !(obj instanceof ThreadIdentifier)) {
      return false;
    }
    ThreadIdentifier other = (ThreadIdentifier) obj;
    return (this.threadID == other.threadID
        && EventID.equalMembershipIds(this.membershipID, other.membershipID));
  }

  @Override
  public int hashCode() {
    final int mult = 37;

    int result = EventID.hashCodeMemberId(membershipID);
    result = mult * result + (int) this.threadID;
    result = mult * result + (int) (this.threadID >>> 32);

    return result;
  }

  public byte[] getMembershipID() {
    return membershipID;
  }

  public long getThreadID() {
    return threadID;
  }

  public static String toDisplayString(long tid) {
    StringBuilder sb = new StringBuilder();
    long lower = Bits.THREAD_ID.extract(tid);
    if (lower != tid) {
      sb.append("0x");
      sb.append(Long.toHexString(tid >> Bits.THREAD_ID.width));
      sb.append("|");
    }
    sb.append(lower);

    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("ThreadId[");
    sb.append("id=").append(membershipID.length).append("bytes; ");
    sb.append(toDisplayString(threadID));
    sb.append("]");

    return sb.toString();
  }

  public String expensiveToString() {
    Object mbr;
    try (ByteArrayDataInput byteArrayDataInput = new ByteArrayDataInput(membershipID)) {
      mbr = InternalDistributedMember.readEssentialData(byteArrayDataInput);
    } catch (Exception e) {
      mbr = membershipID; // punt and use the bytes
    }

    return "ThreadId[" + mbr + "; thread " + toDisplayString(threadID) + "]";
  }

  /**
   * convert fake thread id into real thread id
   *
   * @param tid thread id
   * @return real thread id
   */
  public static long getRealThreadID(long tid) {
    return Bits.THREAD_ID.extract(tid) % MAX_THREAD_PER_CLIENT;
  }

  /**
   * convert fake thread id into real thread id including WAN id
   *
   * @param tid thread id
   * @return real thread id
   */
  public static long getRealThreadIDIncludingWan(long tid) {
    return getRealThreadID(tid) | (tid & WAN_BITS_MASK);
  }

  /**
   * check if current thread id is a fake thread id for putAll
   *
   * @param tid thread id
   * @return whether the thread id is fake
   */
  public static boolean isPutAllFakeThreadID(long tid) {
    return Bits.THREAD_ID.extract(tid) / MAX_THREAD_PER_CLIENT > 0;
  }

  /**
   * check if current thread id is generated by ParallelWAN
   *
   * @param tid thread id
   * @return whether the thread id is generated by ParallelGatewaySender
   */
  public static boolean isParallelWANThreadID(long tid) {
    return WanType.matches(tid) ? true : tid / MAX_THREAD_PER_CLIENT > (MAX_BUCKET_PER_PR + 2);
  }

  /**
   * Checks if the input thread id is a WAN_TYPE thread id
   *
   * @return whether the input thread id is a WAN_TYPE thread id
   */
  public static boolean isWanTypeThreadID(long tid) {
    return WanType.matches(tid);
  }

  /**
   * create a fake id for an operation on the given bucket
   *
   * @return the fake id
   */
  public static long createFakeThreadIDForBulkOp(int bucketNumber, long originatingThreadId) {
    return (MAX_THREAD_PER_CLIENT * (bucketNumber + 1) + originatingThreadId);
  }

  /**
   * create a fake id for an operation on the given bucket
   *
   * @return the fake id
   */
  public static long createFakeThreadIDForParallelGSPrimaryBucket(int bucketId,
      long originatingThreadId, int gatewayIndex) {
    return WanType.PRIMARY.generateWanId(originatingThreadId, bucketId, gatewayIndex);
  }

  /**
   * create a fake id for an operation on the given bucket
   *
   * @return the fake id
   */
  public static long createFakeThreadIDForParallelGSSecondaryBucket(int bucketId,
      long originatingThreadId, int gatewayIndex) {
    return WanType.SECONDARY.generateWanId(originatingThreadId, bucketId, gatewayIndex);
  }

  /**
   * create a fake id for an operation on the given bucket
   *
   * @return the fake id
   */
  public static long createFakeThreadIDForParallelGateway(int index, long originatingThreadId,
      int gatewayIndex) {
    return WanType.PARALLEL.generateWanId(originatingThreadId, index, gatewayIndex);
  }

  /**
   * checks to see if the membership id of this identifier is the same as in the argument
   *
   * @return whether the two IDs are from the same member
   */
  public boolean isSameMember(ThreadIdentifier other) {
    return Arrays.equals(this.membershipID, other.membershipID);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    membershipID = DataSerializer.readByteArray(in);
    threadID = in.readLong();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(membershipID, out);
    out.writeLong(threadID);
  }
}
