/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.util.spring;

/**
 * Replaces org.springframework.shell.support.util.ObjectUtils which
 * is now removed from SPring Shell & the same class is referred from Spring
 * Core. With this we can avoid GemFire member's runtime dependency on Spring
 * Core.
 */
/*
 * Code selectively taken from the original org.springframework.shell.support.util.ObjectUtils
 */
public class ObjectUtils {
  // Constants
  private static final int INITIAL_HASH = 7;
  private static final int MULTIPLIER = 31;
  
  /**
   * Return as hash code for the given object; typically the value of
   * <code>{@link Object#hashCode()}</code>. If the object is an array,
   * this method will delegate to any of the <code>nullSafeHashCode</code>
   * methods for arrays in this class. If the object is <code>null</code>,
   * this method returns 0.
   * 
   * @see #nullSafeHashCode(Object[])
   * @see #nullSafeHashCode(boolean[])
   * @see #nullSafeHashCode(byte[])
   * @see #nullSafeHashCode(char[])
   * @see #nullSafeHashCode(double[])
   * @see #nullSafeHashCode(float[])
   * @see #nullSafeHashCode(int[])
   * @see #nullSafeHashCode(long[])
   * @see #nullSafeHashCode(short[])
   */
  public static int nullSafeHashCode(final Object obj) {
    if (obj == null) {
      return 0;
    }
    if (obj.getClass().isArray()) {
      if (obj instanceof Object[]) {
        return nullSafeHashCode((Object[]) obj);
      }
      if (obj instanceof boolean[]) {
        return nullSafeHashCode((boolean[]) obj);
      }
      if (obj instanceof byte[]) {
        return nullSafeHashCode((byte[]) obj);
      }
      if (obj instanceof char[]) {
        return nullSafeHashCode((char[]) obj);
      }
      if (obj instanceof double[]) {
        return nullSafeHashCode((double[]) obj);
      }
      if (obj instanceof float[]) {
        return nullSafeHashCode((float[]) obj);
      }
      if (obj instanceof int[]) {
        return nullSafeHashCode((int[]) obj);
      }
      if (obj instanceof long[]) {
        return nullSafeHashCode((long[]) obj);
      }
      if (obj instanceof short[]) {
        return nullSafeHashCode((short[]) obj);
      }
    }
    return obj.hashCode();
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array the array from whose elements to calculate the hash code (can be <code>null</code>)
   * @return 0 if the array is <code>null</code>
   */
  public static int nullSafeHashCode(final Object... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (final Object element : array) {
      hash = MULTIPLIER * hash + nullSafeHashCode(element);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final boolean... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + hashCode(array[i]);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final byte... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + array[i];
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final char... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + array[i];
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final double... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + hashCode(array[i]);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final float... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + hashCode(array[i]);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final int... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + array[i];
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final long... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + hashCode(array[i]);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array.
   * 
   * @param array can be <code>null</code>
   * @return 0 if <code>array</code> is <code>null</code>
   */
  public static int nullSafeHashCode(final short... array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    int arraySize = array.length;
    for (int i = 0; i < arraySize; i++) {
      hash = MULTIPLIER * hash + array[i];
    }
    return hash;
  }

  /**
   * Returns the hash code of the given boolean value.
   * 
   * @param bool the boolean for which to return the hash code
   * @return see {@link Boolean#hashCode()}
   */
  public static int hashCode(final boolean bool) {
    return Boolean.valueOf(bool).hashCode();
  }

  /**
   * Return the same value as <code>{@link Double#hashCode()}</code>.
   * 
   * @see Double#hashCode()
   */
  public static int hashCode(final double dbl) {
    long bits = Double.doubleToLongBits(dbl);
    return hashCode(bits);
  }

  /**
   * Return the same value as <code>{@link Float#hashCode()}</code>.
   * 
   * @see Float#hashCode()
   */
  public static int hashCode(final float flt) {
    return Float.floatToIntBits(flt);
  }

  /**
   * Return the same value as <code>{@link Long#hashCode()}</code>.
   * 
   * @see Long#hashCode()
   */
  public static int hashCode(final long lng) {
    return (int) (lng ^ (lng >>> 32));
  }

}
