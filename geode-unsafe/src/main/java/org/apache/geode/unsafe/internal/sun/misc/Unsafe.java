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

package org.apache.geode.unsafe.internal.sun.misc;

import java.lang.reflect.Field;

/**
 * This class wraps the sun.misc.Unsafe class which is only available on Sun JVMs. It is also
 * available on other JVMs (like IBM).
 *
 *
 */
public class Unsafe {

  private final sun.misc.Unsafe unsafe;

  // Cached field offsets for ByteBuffer access
  // These are computed once and reused to avoid repeated reflection
  private static final long BUFFER_ADDRESS_FIELD_OFFSET;
  private static final long BUFFER_CAPACITY_FIELD_OFFSET;

  static {
    long addressOffset = -1;
    long capacityOffset = -1;
    try {
      Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      sun.misc.Unsafe unsafeInstance = (sun.misc.Unsafe) unsafeField.get(null);

      // Get field offsets for Buffer fields
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressOffset = unsafeInstance.objectFieldOffset(addressField);

      Field capacityField = java.nio.Buffer.class.getDeclaredField("capacity");
      capacityOffset = unsafeInstance.objectFieldOffset(capacityField);
    } catch (Exception e) {
      // If initialization fails, offsets remain -1
    }
    BUFFER_ADDRESS_FIELD_OFFSET = addressOffset;
    BUFFER_CAPACITY_FIELD_OFFSET = capacityOffset;
  }

  {
    sun.misc.Unsafe tmp;
    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      tmp = (sun.misc.Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    unsafe = tmp;
  }

  public long objectFieldOffset(Field f) {
    return unsafe.objectFieldOffset(f);
  }

  public int getInt(Object o, long offset) {
    return unsafe.getInt(o, offset);
  }

  public int getIntVolatile(Object o, long offset) {
    return unsafe.getIntVolatile(o, offset);
  }

  /**
   * Returns 4 if this is a 32bit jvm; otherwise 8. Note it does not account for compressed oops.
   */
  public int getAddressSize() {
    return unsafe.addressSize();
  }

  public void putInt(Object o, long offset, int v) {
    unsafe.putInt(o, offset, v);
  }

  public void putIntVolatile(Object o, long offset, int v) {
    unsafe.putIntVolatile(o, offset, v);
  }

  public boolean compareAndSwapInt(Object o, long offset, int expected, int v) {
    return unsafe.compareAndSwapInt(o, offset, expected, v);
  }

  public boolean getBoolean(Object o, long offset) {
    return unsafe.getBoolean(o, offset);
  }

  public void putBoolean(Object o, long offset, boolean v) {
    unsafe.putBoolean(o, offset, v);
  }

  public byte getByte(Object o, long offset) {
    return unsafe.getByte(o, offset);
  }

  public void putByte(Object o, long offset, byte v) {
    unsafe.putByte(o, offset, v);
  }

  public short getShort(Object o, long offset) {
    return unsafe.getShort(o, offset);
  }

  public void putShort(Object o, long offset, short v) {
    unsafe.putShort(o, offset, v);
  }

  public char getChar(Object o, long offset) {
    return unsafe.getChar(o, offset);
  }

  public void putChar(Object o, long offset, char v) {
    unsafe.putChar(o, offset, v);
  }

  public long getLong(Object o, long offset) {
    return unsafe.getLong(o, offset);
  }

  public long getLongVolatile(Object o, long offset) {
    return unsafe.getLongVolatile(o, offset);
  }

  public void putLong(Object o, long offset, long v) {
    unsafe.putLong(o, offset, v);
  }

  public void putLongVolatile(Object o, long offset, long v) {
    unsafe.putLongVolatile(o, offset, v);
  }

  public boolean compareAndSwapLong(Object o, long offset, long expected, long v) {
    return unsafe.compareAndSwapLong(o, offset, expected, v);
  }

  public float getFloat(Object o, long offset) {
    return unsafe.getFloat(o, offset);
  }

  public void putFloat(Object o, long offset, float v) {
    unsafe.putFloat(o, offset, v);
  }

  public double getDouble(Object o, long offset) {
    return unsafe.getDouble(o, offset);
  }

  public void putDouble(Object o, long offset, double v) {
    unsafe.putDouble(o, offset, v);
  }

  public Object getObject(Object o, long offset) {
    return unsafe.getObject(o, offset);
  }

  public void putObject(Object o, long offset, Object v) {
    unsafe.putObject(o, offset, v);
  }

  public Object allocateInstance(Class<?> c) throws InstantiationException {
    return unsafe.allocateInstance(c);
  }

  public long allocateMemory(long size) {
    return unsafe.allocateMemory(size);
  }

  public byte getByte(long addr) {
    return unsafe.getByte(addr);
  }

  public void putByte(long addr, byte value) {
    unsafe.putByte(addr, value);
  }



  public void copyMemory(Object o1, long addr1, Object o2, long addr2, long size) {
    unsafe.copyMemory(o1, addr1, o2, addr2, size);
  }

  public void copyMemory(long src, long dst, long size) {
    unsafe.copyMemory(src, dst, size);
  }

  public void freeMemory(long addr) {
    unsafe.freeMemory(addr);
  }

  public int arrayBaseOffset(Class c) {
    return unsafe.arrayBaseOffset(c);
  }

  public int arrayScaleIndex(Class c) {
    return unsafe.arrayIndexScale(c);
  }

  public long fieldOffset(Field f) {
    return unsafe.objectFieldOffset(f);
  }

  public int getPageSize() {
    return unsafe.pageSize();
  }

  public void setMemory(long addr, long size, byte v) {
    unsafe.setMemory(addr, size, v);
  }

  public int arrayIndexScale(Class<?> arrayClass) {
    return unsafe.arrayIndexScale(arrayClass);
  }

  public Object getObjectVolatile(Object o, long offset) {
    return unsafe.getObjectVolatile(o, offset);
  }

  public boolean compareAndSwapObject(Object o, long offset, Object expected, Object x) {
    return unsafe.compareAndSwapObject(o, offset, expected, x);
  }

  public void putOrderedObject(Object o, long offset, Object x) {
    unsafe.putOrderedObject(o, offset, x);
  }

  /**
   * Gets the native memory address from a DirectByteBuffer using field offset.
   * This method accesses the 'address' field of java.nio.Buffer directly via Unsafe,
   * which does not require --add-opens flags (unlike method reflection with setAccessible()).
   *
   * @param buffer the DirectByteBuffer to get the address from
   * @return the native memory address
   */
  public long getBufferAddress(Object buffer) {
    if (BUFFER_ADDRESS_FIELD_OFFSET == -1) {
      throw new RuntimeException("Buffer address field offset not initialized");
    }
    return unsafe.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
  }

  /**
   * Sets the native memory address for a ByteBuffer using field offset.
   * This allows wrapping an arbitrary memory address as a ByteBuffer.
   *
   * @param buffer the ByteBuffer to set the address for
   * @param address the native memory address
   */
  public void setBufferAddress(Object buffer, long address) {
    if (BUFFER_ADDRESS_FIELD_OFFSET == -1) {
      throw new RuntimeException("Buffer address field offset not initialized");
    }
    unsafe.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
  }

  /**
   * Gets the capacity from a ByteBuffer using field offset.
   *
   * @param buffer the ByteBuffer to get the capacity from
   * @return the buffer capacity
   */
  public int getBufferCapacity(Object buffer) {
    if (BUFFER_CAPACITY_FIELD_OFFSET == -1) {
      throw new RuntimeException("Buffer capacity field offset not initialized");
    }
    return unsafe.getInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET);
  }

  /**
   * Sets the capacity for a ByteBuffer using field offset.
   *
   * @param buffer the ByteBuffer to set the capacity for
   * @param capacity the capacity value
   */
  public void setBufferCapacity(Object buffer, int capacity) {
    if (BUFFER_CAPACITY_FIELD_OFFSET == -1) {
      throw new RuntimeException("Buffer capacity field offset not initialized");
    }
    unsafe.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, capacity);
  }
}
