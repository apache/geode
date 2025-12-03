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
package org.apache.geode.internal.offheap;

import java.nio.ByteBuffer;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.JvmSizeUtils;
import org.apache.geode.unsafe.internal.sun.misc.Unsafe;

/**
 * This class supports allocating and freeing large amounts of addressable memory (i.e. slabs). It
 * also supports using an "address" to operate on the memory. Note that this class's implementation
 * is currently a singleton so all the methods on it are static.
 */
public class AddressableMemoryManager {
  @Immutable
  private static final Unsafe unsafe;
  private static final int ARRAY_BYTE_BASE_OFFSET;
  private static final String reason;
  static {
    Unsafe tmp = null;
    String tmpReason = null;
    try {
      tmp = new Unsafe();
    } catch (RuntimeException | Error e) {
      tmpReason = e.toString();
    }
    reason = tmpReason;
    unsafe = tmp;
    ARRAY_BYTE_BASE_OFFSET = unsafe != null ? unsafe.arrayBaseOffset(byte[].class) : 0;
  }

  public static long allocate(int size) {
    if (unsafe == null) {
      throw new OutOfMemoryError("Off-heap memory is not available because: " + reason);
    }
    try {
      return unsafe.allocateMemory(size);
    } catch (OutOfMemoryError err) {
      String msg = "Failed creating " + size + " bytes of off-heap memory during cache creation.";
      if (err.getMessage() != null && !err.getMessage().isEmpty()) {
        msg += " Cause: " + err.getMessage();
      }
      if (!JvmSizeUtils.is64Bit() && size >= (1024 * 1024 * 1024)) {
        msg +=
            " The JVM looks like a 32-bit one. For large amounts of off-heap memory a 64-bit JVM is needed.";
      }
      throw new OutOfMemoryError(msg);
    }
  }

  public static void free(long addr) {
    unsafe.freeMemory(addr);
  }

  public static Slab allocateSlab(int size) {
    return new SlabImpl(size);
  }

  public static byte readByte(long addr) {
    return unsafe.getByte(addr);
  }

  public static char readChar(long addr) {
    return unsafe.getChar(null, addr);
  }

  public static short readShort(long addr) {
    return unsafe.getShort(null, addr);
  }

  public static int readInt(long addr) {
    return unsafe.getInt(null, addr);
  }

  public static int readIntVolatile(long addr) {
    return unsafe.getIntVolatile(null, addr);
  }

  public static long readLong(long addr) {
    return unsafe.getLong(null, addr);
  }

  public static long readLongVolatile(long addr) {
    return unsafe.getLongVolatile(null, addr);
  }

  public static void writeByte(long addr, byte value) {
    unsafe.putByte(addr, value);
  }

  public static void writeInt(long addr, int value) {
    unsafe.putInt(null, addr, value);
  }

  public static void writeIntVolatile(long addr, int value) {
    unsafe.putIntVolatile(null, addr, value);
  }

  public static boolean writeIntVolatile(long addr, int expected, int value) {
    return unsafe.compareAndSwapInt(null, addr, expected, value);
  }

  public static void writeLong(long addr, long value) {
    unsafe.putLong(null, addr, value);
  }

  public static void writeLongVolatile(long addr, long value) {
    unsafe.putLongVolatile(null, addr, value);
  }

  public static boolean writeLongVolatile(long addr, long expected, long value) {
    return unsafe.compareAndSwapLong(null, addr, expected, value);
  }

  public static void readBytes(long addr, byte[] bytes, int bytesOffset, int size) {
    // Throwing an Error instead of using the "assert" keyword because passing < 0 to
    // copyMemory(...) can lead to a core dump with some JVMs and we don't want to
    // require the -ea JVM flag.
    if (size < 0) {
      throw new AssertionError("Size=" + size + ", but size must be >= 0");
    }

    assert bytesOffset >= 0 : "byteOffset=" + bytesOffset;
    assert bytesOffset + size <= bytes.length : "byteOffset=" + bytesOffset + ",size=" + size
        + ",bytes.length=" + bytes.length;

    if (size == 0) {
      return; // No point in wasting time copying 0 bytes
    }
    unsafe.copyMemory(null, addr, bytes, ARRAY_BYTE_BASE_OFFSET + bytesOffset, size);
  }

  public static void copyMemory(long srcAddr, long dstAddr, long size) {
    unsafe.copyMemory(srcAddr, dstAddr, size);
  }

  public static void writeBytes(long addr, byte[] bytes, int bytesOffset, int size) {
    // Throwing an Error instead of using the "assert" keyword because passing < 0 to
    // copyMemory(...) can lead to a core dump with some JVMs and we don't want to
    // require the -ea JVM flag.
    if (size < 0) {
      throw new AssertionError("Size=" + size + ", but size must be >= 0");
    }

    assert bytesOffset >= 0 : "byteOffset=" + bytesOffset;
    assert bytesOffset + size <= bytes.length : "byteOffset=" + bytesOffset + ",size=" + size
        + ",bytes.length=" + bytes.length;

    if (size == 0) {
      return; // No point in wasting time copying 0 bytes
    }
    unsafe.copyMemory(bytes, ARRAY_BYTE_BASE_OFFSET + bytesOffset, null, addr, size);
  }

  public static void fill(long addr, int size, byte fill) {
    unsafe.setMemory(addr, size, fill);
  }

  /**
   * Returns the address of the Unsafe memory for the first byte of a direct ByteBuffer.
   *
   * This implementation uses Unsafe to access the ByteBuffer's 'address' field directly,
   * which eliminates the need for reflection with setAccessible() and therefore does not
   * require the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
   *
   * If the buffer is not direct or the address cannot be obtained, returns 0.
   *
   * @param bb the ByteBuffer to get the address from
   * @return the native memory address, or 0 if not available
   */
  public static long getDirectByteBufferAddress(ByteBuffer bb) {
    if (!bb.isDirect()) {
      return 0L;
    }
    if (unsafe == null) {
      return 0L;
    }
    try {
      return unsafe.getBufferAddress(bb);
    } catch (Exception e) {
      // If Unsafe access fails, return 0 to indicate failure
      return 0L;
    }
  }

  /**
   * Create a direct byte buffer given its address and size.
   *
   * This implementation creates a standard DirectByteBuffer and then modifies its internal
   * 'address' field to point to the given memory address using Unsafe. This approach uses
   * field-level access via Unsafe which does not require --add-opens flags.
   *
   * The resulting ByteBuffer directly wraps the memory at the given address without copying,
   * making it a zero-copy operation suitable for off-heap memory management.
   *
   * @param address the native memory address to wrap
   * @param size the size of the buffer
   * @return the created direct byte buffer wrapping the address, or null if creation failed
   */
  static ByteBuffer createDirectByteBuffer(long address, int size) {
    if (unsafe == null) {
      return null;
    }
    try {
      // Allocate a small DirectByteBuffer using standard public API
      // We'll reuse this buffer's structure but change its address and capacity
      ByteBuffer buffer = ByteBuffer.allocateDirect(1);

      // Use Unsafe to modify the buffer's internal fields to point to our address
      // This is similar to calling the private DirectByteBuffer(long, int) constructor
      // but using field access instead of constructor reflection
      unsafe.setBufferAddress(buffer, address);
      unsafe.setBufferCapacity(buffer, size);

      // Reset position and limit to match the new capacity
      buffer.clear();
      buffer.limit(size);

      return buffer;
    } catch (Exception e) {
      return null;
    }
  }

}
