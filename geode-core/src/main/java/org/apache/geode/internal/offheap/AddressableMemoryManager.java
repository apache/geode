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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
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

  @SuppressWarnings("rawtypes")
  @MakeNotStatic
  private static volatile Class dbbClass = null;
  @SuppressWarnings("rawtypes")
  @MakeNotStatic
  private static volatile Constructor dbbCtor = null;
  @MakeNotStatic
  private static volatile boolean dbbCreateFailed = false;
  @MakeNotStatic
  private static volatile Method dbbAddressMethod = null;
  @MakeNotStatic
  private static volatile boolean dbbAddressFailed = false;

  /**
   * Returns the address of the Unsafe memory for the first byte of a direct ByteBuffer. If the
   * buffer is not direct or the address can not be obtained return 0.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static long getDirectByteBufferAddress(ByteBuffer bb) {
    if (!bb.isDirect()) {
      return 0L;
    }
    if (dbbAddressFailed) {
      return 0L;
    }
    Method m = dbbAddressMethod;
    if (m == null) {
      Class c = dbbClass;
      if (c == null) {
        try {
          c = Class.forName("java.nio.DirectByteBuffer");
        } catch (ClassNotFoundException e) {
          // throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
          dbbCreateFailed = true;
          dbbAddressFailed = true;
          return 0L;
        }
        dbbClass = c;
      }
      try {
        m = c.getDeclaredMethod("address");
      } catch (NoSuchMethodException | SecurityException e) {
        // throw new IllegalStateException("Could not get method DirectByteBuffer.address()", e);
        dbbClass = null;
        dbbAddressFailed = true;
        return 0L;
      }
      m.setAccessible(true);
      dbbAddressMethod = m;
    }
    try {
      return (Long) m.invoke(bb);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      // throw new IllegalStateException("Could not create an invoke DirectByteBuffer.address()",
      // e);
      dbbClass = null;
      dbbAddressMethod = null;
      dbbAddressFailed = true;
      return 0L;
    }
  }

  /**
   * Create a direct byte buffer given its address and size. The returned ByteBuffer will be direct
   * and use the memory at the given address.
   *
   * @return the created direct byte buffer or null if it could not be created.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  static ByteBuffer createDirectByteBuffer(long address, int size) {
    if (dbbCreateFailed) {
      return null;
    }
    Constructor ctor = dbbCtor;
    if (ctor == null) {
      Class c = dbbClass;
      if (c == null) {
        try {
          c = Class.forName("java.nio.DirectByteBuffer");
        } catch (ClassNotFoundException e) {
          // throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
          dbbCreateFailed = true;
          dbbAddressFailed = true;
          return null;
        }
        dbbClass = c;
      }
      try {
        ctor = c.getDeclaredConstructor(long.class, int.class);
      } catch (NoSuchMethodException | SecurityException e) {
        // throw new IllegalStateException("Could not get constructor DirectByteBuffer(long, int)",
        // e);
        dbbClass = null;
        dbbCreateFailed = true;
        return null;
      }
      ctor.setAccessible(true);
      dbbCtor = ctor;
    }
    try {
      return (ByteBuffer) ctor.newInstance(address, size);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // throw new IllegalStateException("Could not create an instance using DirectByteBuffer(long,
      // int)", e);
      dbbClass = null;
      dbbCtor = null;
      dbbCreateFailed = true;
      return null;
    }
  }

}
