/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * Interprets a one-byte bit field used for entry properties.
 *
 * @author Eric Zoerner
 */
public abstract class EntryBits {
  private static final byte SERIALIZED = 0x1; // persistent bit
  private static final byte INVALID = 0x2; // persistent bit
  private static final byte LOCAL_INVALID = 0x4; // persistent bit
  private static final byte RECOVERED_FROM_DISK = 0x8; // used by DiskId; transient bit
  private static final byte PENDING_ASYNC = 0x10; // used by DiskId; transient bit
  private static final byte TOMBSTONE = 0x40;
  private static final byte WITH_VERSIONS = (byte)0x80; // oplog entry contains versions 

  /**
   * Currently for SQLFabric to deserialize byte[][] eagerly in
   * InitialImageOperation. Can be made a general flag later for all kinds of
   * objects in CachedDeserializable whose serialization is not expensive but
   * that are pretty heavy so creating an intermediate byte[] is expensive.
   * 
   * This is a transient bit that clashes with on-disk persisted bits.
   */
  private static final byte EAGER_DESERIALIZE = 0x20;

  public static boolean isSerialized(byte b) {
    return (b & SERIALIZED) != 0;
  }
  
  public static boolean isInvalid(byte b) {
    return (b & INVALID) != 0;
  }
  
  public static boolean isLocalInvalid(byte b) {
    return (b & LOCAL_INVALID) != 0;
  }
  
  public static boolean isTombstone(byte b) {
    return (b & TOMBSTONE) != 0;
  }
  
  public static boolean isWithVersions(byte b) {
    return (b & WITH_VERSIONS) != 0;
  }

  public static boolean isRecoveredFromDisk(byte b) {
    return (b & RECOVERED_FROM_DISK) != 0;
  }
  public static boolean isPendingAsync(byte b) {
    return (b & PENDING_ASYNC) != 0;
  }
  
  public static boolean isAnyInvalid(byte b) {
    return (b & (INVALID|LOCAL_INVALID)) != 0;
  }
  
  /**
   * If it is not invalid and not local_invalid
   * then we need a value.
   */
  public static boolean isNeedsValue(byte b) {
    return (b & (INVALID|LOCAL_INVALID|TOMBSTONE)) == 0;
  }

  public static boolean isEagerDeserialize(byte b) {
    return (b & EntryBits.EAGER_DESERIALIZE) != 0;
  }

  public static byte setSerialized(byte b, boolean isSerialized) {
    return isSerialized ? (byte)(b | SERIALIZED) : (byte)(b & ~SERIALIZED);
  }
  
  public static byte setInvalid(byte b, boolean isInvalid) {
    return isInvalid ? (byte)(b | INVALID) : (byte)(b & ~INVALID);
  }
  
  public static byte setLocalInvalid(byte b, boolean isLocalInvalid) {
    return isLocalInvalid ? (byte)(b | LOCAL_INVALID) : (byte)(b & ~LOCAL_INVALID);
  }
  
  public static byte setTombstone(byte b, boolean isTombstone) {
    return isTombstone ? (byte)(b | TOMBSTONE) : (byte)(b & ~TOMBSTONE);
  }

  public static byte setWithVersions(byte b, boolean isWithVersions) {
    return isWithVersions ? (byte)(b | WITH_VERSIONS) : (byte)(b & ~WITH_VERSIONS);
  }

  public static byte setRecoveredFromDisk(byte b, boolean isRecoveredFromDisk) {
    return isRecoveredFromDisk ? (byte)(b | RECOVERED_FROM_DISK) : (byte)(b & ~RECOVERED_FROM_DISK);
  }

  public static byte setPendingAsync(byte b, boolean isPendingAsync) {
    return isPendingAsync ? (byte)(b | PENDING_ASYNC) : (byte)(b & ~PENDING_ASYNC);
  }
  /**
   * Returns a byte whose bits are those that need to be written to disk
   */
  public static byte getPersistentBits(byte b) {
    return (byte)(b & (SERIALIZED|INVALID|LOCAL_INVALID|TOMBSTONE|WITH_VERSIONS));
  }

  public static byte setEagerDeserialize(byte b) {
    return (byte)(b | EntryBits.EAGER_DESERIALIZE);
  }

  public static byte clearEagerDeserialize(byte b) {
    return (byte)(b & ~EntryBits.EAGER_DESERIALIZE);
  }
}
