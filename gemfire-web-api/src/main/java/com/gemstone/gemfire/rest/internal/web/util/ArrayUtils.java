/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.util;

/**
 * The ArrayUtils class is an abstract utility class for working with Object arrays.
 * <p/>
 * @author John Blum, Nilkanth Patel
 * @see java.util.Arrays
 * @since 8.0
 */
public abstract class ArrayUtils {

  public static boolean isEmpty(final Object[] array) {
    return (array == null || array.length == 0);
  }

  public static boolean isNotEmpty(final Object[] array) {
    return !isEmpty(array);
  }

  public static int length(final Object[] array) {
    return (array == null ? 0 : array.length);
  }

  public static String toString(final Object... array) {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    if (array != null) {
      for (Object element : array) {
        buffer.append(count++ > 0 ? ", " : "").append(String.valueOf(element));
      }
    }

    buffer.append("]");

    return buffer.toString();
  }

  public static String toString(final String... array) {
    return toString((Object[])array); 
  }
  
}
