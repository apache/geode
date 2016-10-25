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
package org.apache.geode.pdx.internal.unsafe;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * This class wraps the sun.misc.Unsafe class which is only available on Sun JVMs.
 * It is also available on other JVMs (like IBM).
 * 
 *
 */
public class UnsafeWrapper {

  private final Unsafe unsafe;
  {
    Unsafe tmp = null;
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      tmp = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    unsafe = tmp;
  }
  
  public long objectFieldOffset(Field f) {
    return this.unsafe.objectFieldOffset(f);
  }

  public int getInt(Object o, long offset) {
    return this.unsafe.getInt(o, offset);
  }
  public int getIntVolatile(Object o, long offset) {
    return this.unsafe.getIntVolatile(o, offset);
  }

  /**
   * Returns 4 if this is a 32bit jvm; otherwise 8. Note it does not account for compressed oops.
   */
  public int getAddressSize() {
    return this.unsafe.addressSize();
  }

  public void putInt(Object o, long offset, int v) {
    this.unsafe.putInt(o, offset, v);
  }
  public void putIntVolatile(Object o, long offset, int v) {
    this.unsafe.putIntVolatile(o, offset, v);
  }
  public boolean compareAndSwapInt(Object o, long offset, int expected, int v) {
    return this.unsafe.compareAndSwapInt(o, offset, expected, v);
  }

  public boolean getBoolean(Object o, long offset) {
    return this.unsafe.getBoolean(o, offset);
  }

  public void putBoolean(Object o, long offset, boolean v) {
    this.unsafe.putBoolean(o, offset, v);
  }

  public byte getByte(Object o, long offset) {
    return this.unsafe.getByte(o, offset);
  }

  public void putByte(Object o, long offset, byte v) {
    this.unsafe.putByte(o, offset, v);
  }

  public short getShort(Object o, long offset) {
    return this.unsafe.getShort(o, offset);
  }

  public void putShort(Object o, long offset, short v) {
    this.unsafe.putShort(o, offset, v);
  }

  public char getChar(Object o, long offset) {
    return this.unsafe.getChar(o, offset);
  }

  public void putChar(Object o, long offset, char v) {
    this.unsafe.putChar(o, offset, v);
  }

  public long getLong(Object o, long offset) {
    return this.unsafe.getLong(o, offset);
  }
  public long getLongVolatile(Object o, long offset) {
    return this.unsafe.getLongVolatile(o, offset);
  }

  public void putLong(Object o, long offset, long v) {
    this.unsafe.putLong(o, offset, v);
  }
  public void putLongVolatile(Object o, long offset, long v) {
    this.unsafe.putLongVolatile(o, offset, v);
  }
  public boolean compareAndSwapLong(Object o, long offset, long expected, long v) {
    return this.unsafe.compareAndSwapLong(o, offset, expected, v);
  }

  public float getFloat(Object o, long offset) {
    return this.unsafe.getFloat(o, offset);
  }

  public void putFloat(Object o, long offset, float v) {
    this.unsafe.putFloat(o, offset, v);
  }

  public double getDouble(Object o, long offset) {
    return this.unsafe.getDouble(o, offset);
  }

  public void putDouble(Object o, long offset, double v) {
    this.unsafe.putDouble(o, offset, v);
  }

  public Object getObject(Object o, long offset) {
    return this.unsafe.getObject(o, offset);
  }

  public void putObject(Object o, long offset, Object v) {
    this.unsafe.putObject(o, offset, v);
  }
  
  public Object allocateInstance(Class<?> c) throws InstantiationException {
    return this.unsafe.allocateInstance(c);
  }
  
  public long allocateMemory(long size) {
    return this.unsafe.allocateMemory(size);
  }
  public byte getByte(long addr) {
    return this.unsafe.getByte(addr);
  }
  public void putByte(long addr, byte value) {
    this.unsafe.putByte(addr, value);
  }
  
 
  
  public void copyMemory(Object o1, long addr1, Object o2, long addr2, long size) {
    this.unsafe.copyMemory(o1, addr1, o2, addr2, size);
  }
  public void copyMemory(long src, long dst, long size) {
    this.unsafe.copyMemory(src, dst, size);
  }
  public void freeMemory(long addr) {
    this.unsafe.freeMemory(addr);
  }
  public int arrayBaseOffset(Class c) {
    return this.unsafe.arrayBaseOffset(c);
  }
  public int arrayScaleIndex(Class c) {
    return this.unsafe.arrayIndexScale(c);
  }
  public long fieldOffset(Field f) {
    return this.unsafe.objectFieldOffset(f);
  }
  
  public int getPageSize() {
    return this.unsafe.pageSize();
  }

  public void setMemory(long addr, long size, byte v) {
    this.unsafe.setMemory(addr, size, v);
  }
}
