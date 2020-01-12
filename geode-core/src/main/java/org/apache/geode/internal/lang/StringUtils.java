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

package org.apache.geode.internal.lang;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.Token;

/**
 * The StringUtils is an abstract utility class for working with and invoking operations on String
 * literals.
 * <p/>
 *
 * @see java.lang.String
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
@Deprecated
public class StringUtils extends org.apache.commons.lang3.StringUtils {

  public static final String COMMA_DELIMITER = ",";
  public static final String LINE_SEPARATOR = System.getProperty("line.separator");
  public static final String SPACE = " ";

  private static final int MAX_ARRAY_ELEMENTS_TO_CONVERT =
      Integer.getInteger("StringUtils.MAX_ARRAY_ELEMENTS_TO_CONVERT", 16);


  public static String nullifyIfBlank(final String value) {
    return isBlank(value) ? null : value;
  }

  /**
   * Returns only the digits (0..9) from the specified String value.
   * </p>
   *
   * @param value the String value from which to extract digits.
   * @return only the digits from the specified String value. If the String is null or contains no
   *         digits, then this method returns an empty String.
   * @see java.lang.Character#isDigit(char)
   */
  public static String getDigitsOnly(final String value) {
    final StringBuilder buffer = new StringBuilder();

    if (value != null) {
      for (final char chr : value.toCharArray()) {
        if (Character.isDigit(chr)) {
          buffer.append(chr);
        }
      }
    }

    return buffer.toString();
  }

  /**
   * Gets the value of the specified Object as a String. If the Object is null then the first
   * non-null String value from the array of default String value is returned. If the array of
   * String values is null or all the elements in the default String values array are null, then the
   * value of String.valueOf(value) is returned.
   * </p>
   *
   * @param value the Object who's String representation is being evaluated.
   * @param defaultValue an array of default String values to assess if the specified Object value
   *        is null.
   * @return a String representation of the specified Object value or one of the default String
   *         values from the array if the Object value is null. If either the default String array
   *         is null or all the elements are null, then the String value of String.valueOf(value) is
   *         returned.
   * @see java.lang.String#valueOf(Object)
   */
  public static String defaultString(final Object value, final String defaultValue) {
    return value == null ? defaultValue : value.toString();
  }

  public static String defaultString(final Object value) {
    return value == null ? EMPTY : value.toString();
  }

  /**
   * Wraps a line of text to no longer than the specified width, measured by the number of
   * characters in each line, indenting all subsequent lines with the indent. If the indent is null,
   * then an empty String is used.
   * </p>
   *
   * @param line a String containing the line of text to wrap.
   * @param widthInCharacters an integer value indicating the width of each line measured by the
   *        number of characters.
   * @param indent the String value used to indent all subsequent lines.
   * @return the line of text wrapped.
   * @throws IndexOutOfBoundsException if widthInCharacters is less than 0, or there are no word
   *         boundaries within the given width on any given split.
   * @throws NullPointerException if the line of text is null.
   */
  // Can be removed when commons is updated.
  public static String wrap(String line, final int widthInCharacters, String indent) {
    final StringBuilder buffer = new StringBuilder();

    int lineCount = 1;
    int spaceIndex;

    // if indent is null, then do not indent the wrapped lines
    indent = StringUtils.defaultString(indent);

    while (line.length() > widthInCharacters) {
      spaceIndex = line.substring(0, widthInCharacters).lastIndexOf(SPACE);
      buffer.append(lineCount++ > 1 ? indent : EMPTY);
      // throws IndexOutOfBoundsException if spaceIndex is -1, implying no word boundary was found
      // within
      // the given width; this also avoids the infinite loop
      buffer.append(line.substring(0, spaceIndex));
      buffer.append(LINE_SEPARATOR);
      // possible infinite loop if spaceIndex is -1, see comment above
      line = line.substring(spaceIndex + 1);
    }

    buffer.append(lineCount > 1 ? indent : "");
    buffer.append(line);

    return buffer.toString();
  }

  /**
   * Used to convert the given object to a String. If anything goes wrong in this conversion put
   * some info about what went wrong on the result string but do not throw an exception.
   *
   * @param o the object to convert to a string
   * @return the string from of the given object.
   */
  public static String forceToString(Object o) {
    try {
      return objectToString(o, true, MAX_ARRAY_ELEMENTS_TO_CONVERT);
    } catch (RuntimeException ex) {
      return "Conversion to a string failed because " + ex;
    }
  }

  /**
   * Convert an object to a string and return it. Handled CacheDeserializables without having them
   * change the form they store. If deserialization is needed and fails then the string contains a
   * message saying so instead of throwing an exception.
   *
   * @param o the object to convert to a string
   * @param convertArrayContents if true then the contents of the array will be in the string;
   *        otherwise just the array identity
   * @param maxArrayElements if convertArrayContents is true then this parameter limits how many
   *        array elements are converted to the string. After the last converted element "and NNN
   *        more" is used to indicate the number of elements not converted.
   */
  public static String objectToString(Object o, boolean convertArrayContents,
      int maxArrayElements) {
    if (o == null || o == Token.NOT_AVAILABLE) {
      return "null";
    } else if (o instanceof CachedDeserializable) {
      CachedDeserializable cd = (CachedDeserializable) o;
      return cd.getStringForm();
    } else if (convertArrayContents && o.getClass().isArray()) {
      Class<?> eClass = o.getClass();
      if (eClass == byte[].class) {
        return arrayToString((byte[]) o, maxArrayElements);
      } else if (eClass == boolean[].class) {
        return arrayToString((boolean[]) o, maxArrayElements);
      } else if (eClass == char[].class) {
        return arrayToString((char[]) o, maxArrayElements);
      } else if (eClass == short[].class) {
        return arrayToString((short[]) o, maxArrayElements);
      } else if (eClass == int[].class) {
        return arrayToString((int[]) o, maxArrayElements);
      } else if (eClass == long[].class) {
        return arrayToString((long[]) o, maxArrayElements);
      } else if (eClass == float[].class) {
        return arrayToString((float[]) o, maxArrayElements);
      } else if (eClass == double[].class) {
        return arrayToString((double[]) o, maxArrayElements);
      } else {
        return arrayToString((Object[]) o, maxArrayElements);
      }
    } else {
      return o.toString();
    }
  }

  /**
   * Unlike the other arrayToString methods in this class, this method does not surround
   * the array with "[" "]" delimiters.
   */
  public static <T> String arrayToString(T[] array) {
    if (array == null) {
      return "null";
    }
    return Arrays.stream(array).map(String::valueOf).collect(Collectors.joining(", "));
  }

  private static <T> String arrayToString(T[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    String className = a.getClass().getSimpleName();
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    Class componentType = a.getClass().getComponentType();
    if (iMax == -1) {
      return componentType.getSimpleName() + "[]";
    }
    StringBuilder b = new StringBuilder();
    b.append(componentType.getSimpleName());
    b.append('[');
    for (int i = 0;; i++) {
      b.append(String.valueOf(a[i]));
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(boolean[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "boolean[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("boolean[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(byte[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "byte[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("byte[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(char[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "char[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("char[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(short[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "short[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("short[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(int[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "int[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("int[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(long[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "long[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("long[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(float[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "float[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("float[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

  private static String arrayToString(double[] a, int maxArrayElements) {
    if (maxArrayElements < 0) {
      maxArrayElements = 0;
    }
    if (a == null) {
      return "null";
    }
    int iMax = a.length;
    if (iMax > maxArrayElements) {
      iMax = maxArrayElements;
    }
    iMax--;
    if (iMax == -1) {
      return "double[]";
    }
    StringBuilder b = new StringBuilder();
    b.append("double[");
    for (int i = 0;; i++) {
      b.append(a[i]);
      if (i == iMax) {
        int skipCount = a.length - maxArrayElements;
        if (skipCount > 0) {
          if (i > 0) {
            b.append(", ");
          }
          b.append("and ");
          b.append(skipCount);
          b.append(" more");
        }
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }
}
