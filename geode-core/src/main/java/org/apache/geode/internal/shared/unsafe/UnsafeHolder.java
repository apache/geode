/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
/*
 * Changes for SnappyData distributed computational and data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.geode.internal.shared.unsafe;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.LockSupport;

import org.apache.geode.internal.shared.ChannelBufferInputStream;
import org.apache.geode.internal.shared.ChannelBufferOutputStream;
import org.apache.geode.internal.shared.InputStreamChannel;
import org.apache.geode.internal.shared.OutputStreamChannel;

/**
 * Holder for static sun.misc.Unsafe instance and some convenience methods. Use
 * other methods only if {@link UnsafeHolder#hasUnsafe()} returns true;
 *
 * @author swale
 * @since gfxd 1.1
 */
public abstract class UnsafeHolder {

  private static final class Wrapper {

    static final sun.misc.Unsafe unsafe;
    static final boolean unaligned;
    static final Constructor<?> directBufferConstructor;
    static final Field cleanerField;
    static final Field cleanerRunnableField;
    static final Object javaLangRefAccess;
    static final Method handlePendingRefs;

    static {
      sun.misc.Unsafe v;
      Constructor<?> dbConstructor;
      Field cleaner;
      Field runnableField = null;
      try {
        final ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
        // try using "theUnsafe" field
        Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        v = (sun.misc.Unsafe) field.get(null);

        // get the constructor of DirectByteBuffer that accepts a Runnable
        Class<?> cls = Class.forName("java.nio.DirectByteBuffer",
            false, systemLoader);
        dbConstructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
        dbConstructor.setAccessible(true);

        cleaner = cls.getDeclaredField("cleaner");
        cleaner.setAccessible(true);

        // search for the Runnable field in Cleaner
        Class<?> runnableClass = Runnable.class;
        Field[] fields = sun.misc.Cleaner.class.getDeclaredFields();
        for (Field f : fields) {
          if (runnableClass.isAssignableFrom(f.getType())) {
            if (runnableField == null || f.getName().contains("thunk")) {
              f.setAccessible(true);
              runnableField = f;
            }
          }
        }

        Class<?> bitsClass = Class.forName("java.nio.Bits",
            false, systemLoader);
        Method m = bitsClass.getDeclaredMethod("unaligned");
        m.setAccessible(true);
        unaligned = Boolean.TRUE.equals(m.invoke(null));

      } catch (LinkageError le) {
        throw le;
      } catch (Throwable t) {
        throw new ExceptionInInitializerError(t);
      }
      if (v == null) {
        throw new ExceptionInInitializerError("theUnsafe not found");
      }
      if (runnableField == null) {
        throw new ExceptionInInitializerError(
            "DirectByteBuffer cleaner thunk runnable field not found");
      }
      unsafe = v;
      directBufferConstructor = dbConstructor;
      cleanerField = cleaner;
      cleanerRunnableField = runnableField;

      Method m;
      Object langRefAccess;
      try {
        m = sun.misc.SharedSecrets.class.getMethod("getJavaLangRefAccess");
        m.setAccessible(true);
        langRefAccess = m.invoke(null);
        m = langRefAccess.getClass().getMethod("tryHandlePendingReference");
        m.setAccessible(true);
        m.invoke(langRefAccess);
      } catch (Throwable ignored) {
        langRefAccess = null;
        m = null;
      }
      javaLangRefAccess = langRefAccess;
      handlePendingRefs = m;
    }

    static void init() {}
  }

  private static final boolean hasUnsafe;
  // Limit to the chunk copied per Unsafe.copyMemory call to allow for
  // safepoint polling by JVM.
  public static final boolean littleEndian =
      ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  static {
    boolean v;
    try {
      Wrapper.init();
      v = true;
    } catch (LinkageError le) {
      le.printStackTrace();
      v = false;
    }
    hasUnsafe = v;
  }

  private UnsafeHolder() {
    // no instance
  }

  public static boolean hasUnsafe() {
    return hasUnsafe;
  }

  public static int getAllocationSize(int size) {
    // round to word size
    size = ((size + 7) >>> 3) << 3;
    if (size > 0)
      return size;
    else
      throw new BufferOverflowException();
  }

  public static ByteBuffer allocateDirectBuffer(int size,
      FreeMemory.Factory factory) {
    final int allocSize = getAllocationSize(size);
    final ByteBuffer buffer = allocateDirectBuffer(
        getUnsafe().allocateMemory(allocSize), allocSize, factory);
    buffer.limit(size);
    return buffer;
  }

  public static ByteBuffer allocateDirectBuffer(long address, int size,
      FreeMemory.Factory factory) {
    try {
      ByteBuffer buffer = (ByteBuffer) Wrapper.directBufferConstructor
          .newInstance(address, size);
      if (factory != null) {
        sun.misc.Cleaner cleaner = sun.misc.Cleaner.create(buffer,
            factory.newFreeMemory(address, size));
        Wrapper.cleanerField.set(buffer, cleaner);
      }
      return buffer;
    } catch (Exception e) {
      getUnsafe().throwException(e);
      throw new IllegalStateException("unreachable");
    }
  }

  public static long getDirectBufferAddress(ByteBuffer buffer) {
    return ((sun.nio.ch.DirectBuffer) buffer).address();
  }

  // public static ByteBuffer reallocateDirectBuffer(ByteBuffer buffer,
  // int newSize, Class<?> expectedClass, FreeMemory.Factory factory) {
  // sun.nio.ch.DirectBuffer directBuffer = (sun.nio.ch.DirectBuffer)buffer;
  // final long address = directBuffer.address();
  // long newAddress = 0L;
  //
  // newSize = getAllocationSize(newSize);
  // final sun.misc.Cleaner cleaner = directBuffer.cleaner();
  // if (cleaner != null) {
  // // reset the runnable to not free the memory and clean it up
  // try {
  // Object freeMemory = Wrapper.cleanerRunnableField.get(cleaner);
  // if (expectedClass != null && (freeMemory == null ||
  // !expectedClass.isInstance(freeMemory))) {
  // throw new IllegalStateException("Expected class to be " +
  // expectedClass.getName() + " in reallocate but was " +
  // (freeMemory != null ? freeMemory.getClass().getName() : "null"));
  // }
  // // use the efficient realloc call if possible
  // if ((freeMemory instanceof FreeMemory) &&
  // ((FreeMemory)freeMemory).tryFree() != 0L) {
  // newAddress = Wrapper.unsafe.reallocateMemory(address, newSize);
  // }
  // } catch (IllegalAccessException e) {
  // // fallback to full copy
  // }
  // }
  // if (newAddress == 0L) {
  // if (expectedClass != null) {
  // throw new IllegalStateException("Expected class to be " +
  // expectedClass.getName() + " in reallocate but was non-runnable");
  // }
  // newAddress = getUnsafe().allocateMemory(newSize);
  // Platform.copyMemory(null, address, null, newAddress,
  // Math.min(newSize, buffer.limit()));
  // }
  // // clean only after copying is done
  // if (cleaner != null) {
  // cleaner.clean();
  // cleaner.clear();
  // }
  // return allocateDirectBuffer(newAddress, newSize, factory)
  // .order(buffer.order());
  // }

  // /**
  // * Change the runnable field of Cleaner using given factory. The "to"
  // * argument specifies that target Runnable type that factory will produce.
  // * If the existing Runnable already matches "to" then its a no-op.
  // * <p>
  // * The provided {@link BiConsumer} is used to apply any action before actually
  // * changing the runnable field with the boolean argument indicating whether
  // * the current field matches "from" or if it is something else.
  // */
  // public static void changeDirectBufferCleaner(
  // ByteBuffer buffer, int size, Class<? extends FreeMemory> from,
  // Class<? extends FreeMemory> to, FreeMemory.Factory factory,
  // final BiConsumer<String, Object> changeOwner) throws IllegalAccessException {
  // sun.nio.ch.DirectBuffer directBuffer = (sun.nio.ch.DirectBuffer)buffer;
  // final sun.misc.Cleaner cleaner = directBuffer.cleaner();
  // if (cleaner != null) {
  // // change the runnable
  // final Field runnableField = Wrapper.cleanerRunnableField;
  // Object runnable = runnableField.get(cleaner);
  // // skip if it already matches the target Runnable type
  // if (!to.isInstance(runnable)) {
  // if (changeOwner != null) {
  // if (from.isInstance(runnable)) {
  // changeOwner.accept(((FreeMemory)runnable).objectName(), runnable);
  // } else {
  // changeOwner.accept(null, runnable);
  // }
  // }
  // Runnable newFree = factory.newFreeMemory(directBuffer.address(), size);
  // runnableField.set(cleaner, newFree);
  // }
  // } else {
  // throw new IllegalAccessException(
  // "ByteBuffer without a Cleaner cannot be marked for storage");
  // }
  // }

  /**
   * Release explicitly assuming passed ByteBuffer is a direct one. Avoid using
   * this directly rather use BufferAllocator.allocate/release where possible.
   */
  public static void releaseDirectBuffer(ByteBuffer buffer) {
    sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
    if (cleaner != null) {
      cleaner.clean();
      cleaner.clear();
    }
    buffer.rewind().limit(0);
  }

  public static void releasePendingReferences() {
    // commented code intended to be invoked by reflection for platforms
    // that may not have the requisite classes (e.g. Mac default JDK)
    /*
     * final sun.misc.JavaLangRefAccess refAccess =
     * sun.misc.SharedSecrets.getJavaLangRefAccess();
     * while (refAccess.tryHandlePendingReference()) ;
     */
    final Method handlePendingRefs = Wrapper.handlePendingRefs;
    if (handlePendingRefs != null) {
      try {
        // retry while helping enqueue pending Cleaner Reference objects
        // noinspection StatementWithEmptyBody
        while ((Boolean) handlePendingRefs.invoke(Wrapper.javaLangRefAccess));
      } catch (Exception ignored) {
        // ignore any exceptions in releasing pending references
      }
    }
  }

  public static sun.misc.Unsafe getUnsafe() {
    return Wrapper.unsafe;
  }

  public static boolean tryMonitorEnter(Object obj, boolean checkSelf) {
    if (checkSelf && Thread.holdsLock(obj)) {
      return false;
    } else if (!getUnsafe().tryMonitorEnter(obj)) {
      // try once more after a small wait
      LockSupport.parkNanos(100L);
      if (!getUnsafe().tryMonitorEnter(obj)) {
        return false;
      }
    }
    return true;
  }

  public static void monitorEnter(Object obj) {
    getUnsafe().monitorEnter(obj);
  }

  public static void monitorExit(Object obj) {
    getUnsafe().monitorExit(obj);
  }

  @SuppressWarnings("resource")
  public static InputStreamChannel newChannelBufferInputStream(
      ReadableByteChannel channel, int bufferSize) throws IOException {
    return (hasUnsafe
        ? new ChannelBufferUnsafeInputStream(channel, bufferSize)
        : new ChannelBufferInputStream(channel, bufferSize));
  }

  @SuppressWarnings("resource")
  public static OutputStreamChannel newChannelBufferOutputStream(
      WritableByteChannel channel, int bufferSize) throws IOException {
    return (hasUnsafe
        ? new ChannelBufferUnsafeOutputStream(channel, bufferSize)
        : new ChannelBufferOutputStream(channel, bufferSize));
  }

  // @SuppressWarnings("resource")
  // public static InputStreamChannel newChannelBufferFramedInputStream(
  // ReadableByteChannel channel, int bufferSize) throws IOException {
  // return (hasUnsafe
  // ? new ChannelBufferUnsafeFramedInputStream(channel, bufferSize)
  // : new ChannelBufferFramedInputStream(channel, bufferSize));
  // }
  //
  // @SuppressWarnings("resource")
  // public static OutputStreamChannel newChannelBufferFramedOutputStream(
  // WritableByteChannel channel, int bufferSize) throws IOException {
  // return (hasUnsafe
  // ? new ChannelBufferUnsafeFramedOutputStream(channel, bufferSize)
  // : new ChannelBufferFramedOutputStream(channel, bufferSize));
  // }

  /**
   * Checks that the range described by {@code offset} and {@code size}
   * doesn't exceed {@code arrayLength}.
   */
  public static void checkBounds(int arrayLength, int offset, int len) {
    if ((offset | len) < 0 || offset > arrayLength ||
        arrayLength - offset < len) {
      throw new ArrayIndexOutOfBoundsException("Array index out of range: " +
          "length=" + arrayLength + " offset=" + offset + " length=" + len);
    }
  }
}
