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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.internal.SharedLibrary;
import com.gemstone.gemfire.pdx.internal.unsafe.UnsafeWrapper;

/**
 * Represents a single addressable chunk of off-heap memory. The size specifies
 * the number of bytes stored at the address.
 * 
 * @since 9.0
 */
public class UnsafeMemoryChunk implements AddressableMemoryChunk {
  private static final UnsafeWrapper unsafe;
  private static final int ARRAY_BYTE_BASE_OFFSET;
  private static String reason;
  static {
	UnsafeWrapper tmp = null;
	try {
	  tmp = new UnsafeWrapper();
	  reason = null;
	} catch (RuntimeException ignore) {
      reason = ignore.toString();
	} catch (Error ignore) {
      reason = ignore.toString();
	}
	unsafe = tmp;
    ARRAY_BYTE_BASE_OFFSET = unsafe != null ? unsafe.arrayBaseOffset(byte[].class) : 0;
  }
  
  private final long data;
  private final int size;
  
  public UnsafeMemoryChunk(int size) {
	if (unsafe == null) {
      throw new OutOfMemoryError("Off-heap memory is not available because: " + reason);
	}
    try {
    this.data = unsafe.allocateMemory(size);
    this.size = size;
    } catch (OutOfMemoryError err) {
      String msg = "Failed creating " + size + " bytes of off-heap memory during cache creation.";
      if (err.getMessage() != null && !err.getMessage().isEmpty()) {
        msg += " Cause: " + err.getMessage();
      }
      if (!SharedLibrary.is64Bit() && size >= (1024*1024*1024)) {
        msg += " The JVM looks like a 32-bit one. For large amounts of off-heap memory a 64-bit JVM is needed.";
      }
      throw new OutOfMemoryError(msg);
    }
  }

  @Override
  public int getSize() {
    return (int)this.size;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.offheap.AddressableMemoryChunk#getMemoryAddress()
   */
  @Override
  public long getMemoryAddress() {
    return this.data;
  }
  
  public static byte readAbsoluteByte(long addr) {
    return unsafe.getByte(addr);
  }
  public static char readAbsoluteChar(long addr) {
    return unsafe.getChar(null, addr);
  }
  public static short readAbsoluteShort(long addr) {
    return unsafe.getShort(null, addr);
  }
  public static int readAbsoluteInt(long addr) {
    return unsafe.getInt(null, addr);
  }
  public static int readAbsoluteIntVolatile(long addr) {
    return unsafe.getIntVolatile(null, addr);
  }
  public static long readAbsoluteLong(long addr) {
    return unsafe.getLong(null, addr);
  }
  public static long readAbsoluteLongVolatile(long addr) {
    return unsafe.getLongVolatile(null, addr);
  }

  @Override
  public byte readByte(int offset) {
    return readAbsoluteByte(this.data+offset);
  }

  public static void writeAbsoluteByte(long addr, byte value) {
    unsafe.putByte(addr, value);
  }
       
  public static void writeAbsoluteInt(long addr, int value) {
    unsafe.putInt(null, addr, value);
  }
  public static void writeAbsoluteIntVolatile(long addr, int value) {
    unsafe.putIntVolatile(null, addr, value);
  }
  public static boolean writeAbsoluteIntVolatile(long addr, int expected, int value) {
    return unsafe.compareAndSwapInt(null, addr, expected, value);
  }
  public static void writeAbsoluteLong(long addr, long value) {
    unsafe.putLong(null, addr, value);
  }
  public static void writeAbsoluteLongVolatile(long addr, long value) {
    unsafe.putLongVolatile(null, addr, value);
  }
  public static boolean writeAbsoluteLongVolatile(long addr, long expected, long value) {
    return unsafe.compareAndSwapLong(null, addr, expected, value);
  }

  @Override
  public void writeByte(int offset, byte value) {
    writeAbsoluteByte(this.data+offset, value);
  }

  @Override
  public void readBytes(int offset, byte[] bytes) {
    readBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(int offset, byte[] bytes) {
    writeBytes(offset, bytes, 0, bytes.length);
  }

  public static void readAbsoluteBytes(long addr, byte[] bytes, int bytesOffset, int size) {
    // Throwing an Error instead of using the "assert" keyword because passing < 0 to
    // copyMemory(...) can lead to a core dump with some JVMs and we don't want to
    // require the -ea JVM flag.
    if (size < 0) {
      throw new AssertionError("Size=" + size + ", but size must be >= 0");
    }
    
    assert bytesOffset >= 0 : "byteOffset=" + bytesOffset;
    assert bytesOffset + size <= bytes.length : "byteOffset=" + bytesOffset + ",size=" + size + ",bytes.length=" + bytes.length;
    
    if (size == 0) {
      return; // No point in wasting time copying 0 bytes
    }
    unsafe.copyMemory(null, addr, bytes, ARRAY_BYTE_BASE_OFFSET+bytesOffset, size);
  }

  @Override
  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    readAbsoluteBytes(this.data+offset, bytes, bytesOffset, size);
  }

  public static void copyMemory(long srcAddr, long dstAddr, long size) {
    unsafe.copyMemory(srcAddr, dstAddr, size);
  }
  
  public static void writeAbsoluteBytes(long addr, byte[] bytes, int bytesOffset, int size) {
    // Throwing an Error instead of using the "assert" keyword because passing < 0 to
    // copyMemory(...) can lead to a core dump with some JVMs and we don't want to
    // require the -ea JVM flag.
    if (size < 0) {
      throw new AssertionError("Size=" + size + ", but size must be >= 0");
    }

    assert bytesOffset >= 0 : "byteOffset=" + bytesOffset;
    assert bytesOffset + size <= bytes.length : "byteOffset=" + bytesOffset + ",size=" + size + ",bytes.length=" + bytes.length;
    
    if (size == 0) {
      return; // No point in wasting time copying 0 bytes
    }
    unsafe.copyMemory(bytes, ARRAY_BYTE_BASE_OFFSET+bytesOffset, null, addr, size);
  }

  public static void fill(long addr, int size, byte fill) {
    unsafe.setMemory(addr, size, fill);
  }
  
  @Override
  public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    writeAbsoluteBytes(this.data+offset, bytes, bytesOffset, size);
  }

  @Override
  public void release() {
    unsafe.freeMemory(this.data);
  }

  @Override
  public void copyBytes(int src, int dst, int size) {
    unsafe.copyMemory(this.data+src, this.data+dst, size);
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("{");
    sb.append("MemoryAddress=").append(getMemoryAddress());
    sb.append(", Size=").append(getSize());
    sb.append("}");
    return sb.toString();
  }
}
