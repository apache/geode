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
package org.apache.geode.internal.size;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.JvmSizeUtils;
import org.apache.geode.unsafe.internal.sun.misc.Unsafe;

/**
 * Figure out the size of an object using reflection. This class does not follow any object
 * references, it just calculates the size of a flat object.
 *
 *
 */
public class ReflectionSingleObjectSizer implements SingleObjectSizer {
  @Immutable
  private static final ReflectionSingleObjectSizer INSTANCE = new ReflectionSingleObjectSizer();

  public static final int REFERENCE_SIZE = JvmSizeUtils.getReferenceSize();
  public static final int OBJECT_SIZE = JvmSizeUtils.getObjectHeaderSize();

  @Immutable
  private static final Unsafe unsafe;
  static {
    Unsafe tmp = null;
    try {
      tmp = new Unsafe();
    } catch (RuntimeException | Error ignore) {
    }
    unsafe = tmp;
  }

  @Override
  public long sizeof(Object object) {
    return sizeof(object, true);
  }

  public long sizeof(Object object, boolean roundResult) {
    Class<?> clazz = object.getClass();
    long size;
    if (clazz.isArray()) {
      if (unsafe != null) {
        size = unsafe.arrayBaseOffset(clazz);
        int arrayLength = Array.getLength(object);
        if (arrayLength > 0) {
          int typeSize = unsafe.arrayScaleIndex(clazz);
          if (typeSize == 0) {
            // the javadocs say that arrayScaleIndex may return 0.
            // If it did then we use sizeType.
            typeSize = sizeType(clazz.getComponentType());
          }
          size += (long) arrayLength * typeSize;
        }
      } else {
        // not as accurate but does not use unsafe
        size = OBJECT_SIZE + 4 /* for array length */;
        int arrayLength = Array.getLength(object);
        if (arrayLength > 0) {
          size += (long) arrayLength * sizeType(clazz.getComponentType());
        }
      }
      if (roundResult) {
        size = roundUpSize(size);
      }
      return size;
    } else {
      return sizeof(clazz, roundResult);
    }
  }

  public static long sizeof(Class<?> clazz) {
    return sizeof(clazz, true);
  }

  /**
   * Since unsafe.fieldOffset(Field) will give us the offset to the first byte of that field all we
   * need to do is find which of the non-static declared fields has the greatest offset.
   */
  public static long sizeof(Class<?> clazz, boolean roundResult) {
    Assert.assertTrue(!clazz.isArray());
    long size;
    if (unsafe != null) {
      Field lastField = null;
      long lastFieldOffset = 0;
      do {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
          if (!Modifier.isStatic(field.getModifiers())) {
            long offset = unsafe.fieldOffset(field);
            if (offset >= lastFieldOffset) {
              lastFieldOffset = offset;
              lastField = field;
            }
          }
        }
        if (lastField != null) {
          // if we have found a field in a subclass then one of them will be the last field
          // so just break without looking at super class fields.
          break;
        }
        clazz = clazz.getSuperclass();
      } while (clazz != null);

      if (lastField != null) {
        size = lastFieldOffset + sizeType(lastField.getType());
      } else {
        // class with no fields
        size = OBJECT_SIZE;
      }
    } else {
      // This code is not as accurate as unsafe but gives an estimate of memory used.
      // If it is wrong it will always under estimate because it does not account
      // for any of the field alignment that the jvm does.
      size = OBJECT_SIZE;
      do {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
          if (!Modifier.isStatic(field.getModifiers())) {
            size += sizeType(field.getType());
          }
        }
        clazz = clazz.getSuperclass();
      } while (clazz != null);
    }
    if (roundResult) {
      size = roundUpSize(size);
    }
    return size;
  }

  public static long roundUpSize(long size) {
    // Round up to the nearest 8 bytes. Experimentally, this
    // is what we've seen the sun 32 bit VM do with object size.
    // See https://wiki.gemstone.com/display/rusage/Per+Entry+Overhead
    long remainder = size % 8;
    if (remainder != 0) {
      size = size - remainder + 8;
    }
    return size;
  }

  private static int sizeType(Class<?> t) {

    if (t == Boolean.TYPE) {
      return 1;
    } else if (t == Byte.TYPE) {
      return 1;
    } else if (t == Character.TYPE) {
      return 2;
    } else if (t == Short.TYPE) {
      return 2;
    } else if (t == Integer.TYPE) {
      return 4;
    } else if (t == Long.TYPE) {
      return 8;
    } else if (t == Float.TYPE) {
      return 4;
    } else if (t == Double.TYPE) {
      return 8;
    } else if (t == Void.TYPE) {
      return 0;
    } else {
      return REFERENCE_SIZE;
    }
  }

  public static ReflectionSingleObjectSizer getInstance() {
    return INSTANCE;
  }

}
