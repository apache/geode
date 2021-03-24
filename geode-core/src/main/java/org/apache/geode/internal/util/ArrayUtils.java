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

package org.apache.geode.internal.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 *
 * Handle some simple editing of fixed-length arrays.
 *
 * TODO use Java 1.5 template classes to simplify this interface
 *
 */
public abstract class ArrayUtils {

  /**
   * Gets the element at index in the array in a bound-safe manner. If index is not a valid index in
   * the given array, then the default value is returned.
   * <p/>
   *
   * @param <T> the class type of the elements in the array.
   * @param array the array from which the element at index is retrieved.
   * @param index the index into the array to retrieve the element.
   * @param defaultValue the default value of element type to return in the event that the array
   *        index is invalid.
   * @return the element at index from the array or the default value if the index is invalid.
   */
  public static <T> T getElementAtIndex(T[] array, int index, T defaultValue) {
    try {
      return array[index];
    } catch (ArrayIndexOutOfBoundsException ignore) {
      return defaultValue;
    }
  }

  /**
   * Gets the first element from the given array or null if the array reference is null or the array
   * length is 0.
   * <p/>
   *
   * @param <T> the Class type of the elements in the array.
   * @param array the array of elements from which to retrieve the first element.
   * @return the first element from the array or null if either the array reference is null or the
   *         array length is 0.
   */
  public static <T> T getFirst(final T... array) {
    return (array != null && array.length > 0 ? array[0] : null);
  }

  /**
   * Converts the specified Object array into a String representation.
   * <p/>
   *
   * @param array the Object array of elements to convert to a String.
   * @return a String representation of the Object array.
   * @see java.lang.StringBuilder
   */
  public static String toString(final Object... array) {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    if (array != null) {
      for (final Object element : array) {
        buffer.append(count++ > 0 ? ", " : StringUtils.EMPTY).append(element);
      }
    }

    buffer.append("]");

    return buffer.toString();
  }

  public static String toString(final String... array) {
    return toString((Object[]) array);
  }

  /**
   * Insert an element into an array. The element is inserted at the given position, all elements
   * afterwards are moved to the right.
   *
   * @param originalArray array to insert into
   * @param pos position at which to insert the element
   * @param element element to add
   * @return the new array
   */
  public static Object[] insert(Object[] originalArray, int pos, Object element) {
    Object[] newArray = (Object[]) java.lang.reflect.Array
        .newInstance(originalArray.getClass().getComponentType(), originalArray.length + 1);

    // Test Cases (proof of correctness by examining corner cases)
    // 1) A B C D insert at 0: expect X A B C D
    // 2) A B C D insert at 2: expect A B X C D
    // 3) A B C D insert at 4: expect A B C D X

    // copy everything before the given position
    if (pos > 0) {
      System.arraycopy(originalArray, 0, newArray, 0, pos); // does not copy originalArray[pos],
                                                            // where we insert
    }

    // 1) A B C D insert at 0: no change, ". . . . ."
    // 2) A B C D insert at 2: copy "A B", "A B . . ."
    // 3) A B C D insert at 4: copy "A B C D", "A B C D ."

    // insert
    newArray[pos] = element;

    // 1) A B C D insert at 0: "X . . . ."
    // 2) A B C D insert at 2: "A B X . ."
    // 3) A B C D insert at 4: "A B C D X" (all done)

    // copy remaining elements
    if (pos < originalArray.length) {
      System.arraycopy(originalArray, pos, // originalArray[pos] first element copied
          newArray, pos + 1, // newArray[pos + 1] first destination
          originalArray.length - pos); // number of elements left
    }

    // 1) A B C D insert at 0: "A B C D" copied at 1: "X A B C D"
    // 2) A B C D insert at 2: "C D" copied at 3: "A B X C D"
    // 3) A B C D insert at 4: no change
    return newArray;
  }

  /**
   * Remove element from an array. The element is removed at the specified position, and all
   * remaining elements are moved to the left.
   *
   * @param originalArray array to remove from
   * @param pos position to remove
   * @return the new array
   */
  public static Object[] remove(Object[] originalArray, int pos) {
    Object[] newArray = (Object[]) java.lang.reflect.Array
        .newInstance(originalArray.getClass().getComponentType(), originalArray.length - 1);

    // Test cases: (proof of correctness)
    // 1) A B C D E remove 0: expect "B C D E"
    // 2) A B C D E remove 2: expect "A B D E"
    // 3) A B C D E remove 4: expect "A B C D"

    // Copy everything before
    if (pos > 0) {
      System.arraycopy(originalArray, 0, newArray, 0, pos); // originalArray[pos - 1] is last
                                                            // element copied
    }

    // 1) A B C D E remove 0: no change, ". . . ."
    // 2) A B C D E remove 2: "A B" copied at beginning: "A B . ."
    // 3) A B C D E remove 4: "A B C D" copied (all done)

    // Copy everything after
    if (pos < originalArray.length - 1) {
      System.arraycopy(originalArray, pos + 1, // originalArray[pos + 1] is first element copied
          newArray, pos, // first position to copy into
          originalArray.length - 1 - pos);
    }

    // 1) A B C D E remove 0: "B C D E" copied into to position 0
    // 2) A B C D E remove 2: "D E" copied into position 2: "A B D E"
    // 3) A B C D E remove 4: no change
    return newArray;
  }

  public static String objectRefString(Object obj) {
    return obj != null
        ? obj.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(obj))
        : "(null)";
  }

  public static void objectRefString(Object obj, StringBuilder sb) {
    if (obj != null) {
      sb.append(obj.getClass().getSimpleName()).append('@')
          .append(Integer.toHexString(System.identityHashCode(obj)));
    } else {
      sb.append("(null)");
    }
  }

  /** Get proper string for an object including arrays. */
  public static String objectString(Object obj) {
    StringBuilder sb = new StringBuilder();
    objectString(obj, sb);
    return sb.toString();
  }

  /** Get proper string for an object including arrays. */
  public static void objectString(Object obj, StringBuilder sb) {
    if (obj instanceof Object[]) {
      sb.append('(');
      boolean first = true;
      for (Object o : (Object[]) obj) {
        if (!first) {
          sb.append(',');
        } else {
          first = false;
        }
        objectString(o, sb);
      }
      sb.append(')');
    } else {
      objectStringWithBytes(obj, sb);
    }
  }

  /**
   * Get proper string for an an object including arrays with upto one dimension of arrays.
   */
  public static String objectStringNonRecursive(@Unretained Object obj) {
    StringBuilder sb = new StringBuilder();
    objectStringNonRecursive(obj, sb);
    return sb.toString();
  }

  public static boolean areByteArrayArrayEquals(byte[][] v1, byte[][] v2) {
    boolean areEqual = false;
    if (v1.length == v2.length) {
      areEqual = true;
      for (int index = 0; index < v1.length; ++index) {
        if (!Arrays.equals(v1[index], v2[index])) {
          areEqual = false;
          break;
        }
      }
    }
    return areEqual;
  }

  /**
   * Get proper string for an an object including arrays with upto one dimension of arrays.
   */
  public static void objectStringNonRecursive(@Unretained Object obj, StringBuilder sb) {
    if (obj instanceof Object[]) {
      sb.append('(');
      boolean first = true;
      for (Object o : (Object[]) obj) {
        if (!first) {
          sb.append(',');
          sb.append(o);
        } else {
          first = false;
          // show the first byte[] for byte[][] storage
          objectStringWithBytes(o, sb);
        }
      }
      sb.append(')');
    } else {
      objectStringWithBytes(obj, sb);
    }
  }

  private static void objectStringWithBytes(@Unretained Object obj, StringBuilder sb) {
    if (obj instanceof byte[]) {
      sb.append('(');
      boolean first = true;
      final byte[] bytes = (byte[]) obj;
      int numBytes = 0;
      for (byte b : bytes) {
        if (!first) {
          sb.append(',');
        } else {
          first = false;
        }
        sb.append(b);
        // terminate with ... for large number of bytes
        if (numBytes++ >= 5000 && numBytes < bytes.length) {
          sb.append(" ...");
          break;
        }
      }
      sb.append(')');
    } else {
      sb.append(obj);
    }
  }

  /**
   * Check if two objects, possibly null, are equal. Doesn't really belong to this class...
   */
  public static boolean objectEquals(Object o1, Object o2) {
    if (o1 == o2) {
      return true;
    }
    if (o1 == null) {
      return false;
    }
    return o1.equals(o2);
  }

  /**
   * Converts the primitive int array into an Integer wrapper object array.
   * </p>
   *
   * @param array the primitive int array to convert into an Integer wrapper object array.
   * @return an Integer array containing the values from the elements in the primitive int array.
   */
  public static Integer[] toIntegerArray(final int[] array) {
    final Integer[] integerArray = new Integer[array == null ? 0 : array.length];

    if (array != null) {
      for (int index = 0; index < array.length; index++) {
        integerArray[index] = array[index];
      }
    }

    return integerArray;
  }

  /**
   * Converts a double byte array into a double Byte array.
   *
   * @param array the double byte array to convert into double Byte array
   * @return a double array of Byte objects containing values from the double byte array
   */
  public static Byte[][] toByteArray(final byte[][] array) {
    if (array == null) {
      return null;
    }
    final Byte[][] byteArray = new Byte[array.length][];
    for (int i = 0; i < array.length; i++) {
      byteArray[i] = new Byte[array[i].length];
      for (int j = 0; j < array[i].length; j++) {
        byteArray[i][j] = array[i][j];
      }
    }
    return byteArray;
  }

  /**
   * Converts a double Byte array into a double byte array.
   *
   * @param byteArray the double Byte array to convert into a double byte array
   * @return a double byte array containing byte values from the double Byte array
   */
  public static byte[][] toBytes(final Byte[][] byteArray) {
    if (byteArray == null) {
      return null;
    }
    final byte[][] array = new byte[byteArray.length][];
    for (int i = 0; i < byteArray.length; i++) {
      array[i] = new byte[byteArray[i].length];
      for (int j = 0; j < byteArray[i].length; j++) {
        array[i][j] = byteArray[i][j];
      }
    }
    return array;
  }

  /**
   * Use this instead of Arrays.asList(T... array) when you need a modifiable List.
   *
   * Returns a modifiable list containing the elements of the specified array.
   *
   * <p>
   * Example usage:
   *
   * <pre>
   * List&lt;String&gt; stooges = Arrays.asList("Larry", "Moe", "Curly");
   * stooges.remove("Curly");
   * </pre>
   *
   * @param <T> the class of the objects in the array
   * @param array the array of elements to be added to the list
   * @return a list containing the elements of the specified array
   */
  public static <T> List<T> asList(T... array) {
    return new ArrayList<>(Arrays.asList(array));
  }

  /**
   * Assigns the specified typed reference to each element of the specified
   * typed array.
   *
   * @param array the array to be filled
   * @param value the value to be stored in all elements of the array
   * @return the specified typed array filled with the specified typed reference
   * @throws ArrayStoreException if the specified value is not of a runtime type that can be stored
   *         in the specified array
   */
  public static <T> T[] fill(T[] array, T value) {
    for (int i = 0, length = array.length; i < length; i++) {
      array[i] = value;
    }
    return array;
  }
}
