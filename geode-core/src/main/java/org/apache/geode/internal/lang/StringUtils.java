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

package com.gemstone.gemfire.internal.lang;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.Token;

/**
 * The StringUtils is an abstract utility class for working with and invoking operations on String literals.
 * <p/>
 * @see java.lang.String
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public abstract class StringUtils {

  public static final String COMMA_DELIMITER = ",";
  public static final String EMPTY_STRING = "";
  public static final String LINE_SEPARATOR = System.getProperty("line.separator");
  public static final String SPACE = " ";
  public static final String UTF_8 = "UTF-8";

  public static final String[] EMPTY_STRING_ARRAY = new String[0];

  public static final String[] SPACES = {
    "",
    " ",
    "  ",
    "   ",
    "    ",
    "     ",
    "      ",
    "       ",
    "        ",
    "         ",
    "          "
  };

  /**
   * Concatenates all Objects in the array into a single String by calling toString on the Object.
   * </p>
   * @param values the Object elements of the array to be concatenated into the String.
   * @return a single String with all the individual Objects in the array concatenated.
   * @see #concat(Object[], String)
   */
  public static String concat(final Object... values) {
    return concat(values, EMPTY_STRING);
  }

  /**
   * Concatenates all Objects in the array into a single String using the Object's toString method, delimited by the
   * specified delimiter.
   * </p>
   * @param values an array of Objects to concatenate into a single String value.
   * @param delimiter the String value to use as a separator between the individual Object values.  If delimiter is
   * null, then a empty String is used.
   * @return a single String with all the individual Objects of the array concatenated together, separated by the
   * specified delimiter.
   * @see java.lang.Object#toString()
   * @see java.lang.StringBuilder
   */
  public static String concat(final Object[] values, String delimiter) {
    delimiter = ObjectUtils.defaultIfNull(delimiter, EMPTY_STRING);

    final StringBuilder buffer = new StringBuilder();
    int count = 0;

    if (values != null) {
      for (Object value : values) {
        buffer.append(count++ > 0 ? delimiter : EMPTY_STRING);
        buffer.append(value);
      }
    }

    return buffer.toString();
  }

  /**
   * Returns the first non-null, non-empty and non-blank String value in the array of String values.
   * </p>
   * @param values an array of String values, usually consisting of the preferred value followed by default values
   * if any value in the array of String values is null, empty or blank.
   * @return the first non-null, non-empty and non-blank String value in the array of Strings.  If all values are
   * either null, empty or blank then null is returned.
   * @see #isBlank(String)
   */
  public static String defaultIfBlank(final String... values) {
    if (values != null) {
      for (final String value : values) {
        if (!isBlank(value)) {
          return value;
        }
      }
    }

    return null;
  }

  /**
   * Returns only the digits (0..9) from the specified String value.
   * </p>
   * @param value the String value from which to extract digits.
   * @return only the digits from the specified String value.  If the String is null or contains no digits,
   * then this method returns an empty String.
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
   * Returns only the letters (a..zA..Z) from the specified String value.
   * </p>
   * @param value the String value from which to extract letters.
   * @return only the letters from the specified String value.  If the String is null or contains no letters,
   * then this method returns an empty String.
   * @see java.lang.Character#isLetter(char)
   */
  public static String getLettersOnly(final String value) {
    final StringBuilder buffer = new StringBuilder();

    if (value != null) {
      for (final char chr : value.toCharArray()) {
        if (Character.isLetter(chr)) {
          buffer.append(chr);
        }
      }
    }

    return buffer.toString();
  }

  /**
   * Gets a number of spaces determined by number.
   * </p>
   * @param number an integer value indicating the number of spaces to return.
   * @return a String value containing a number of spaces given by number.
   */
  public static String getSpaces(int number) {
    final StringBuilder spaces = new StringBuilder(SPACES[Math.min(number, SPACES.length - 1)]);

    do {
      number -= (SPACES.length -1);
      number = Math.max(number, 0);
      spaces.append(SPACES[Math.min(number, SPACES.length - 1)]);
    }
    while (number > 0);

    return spaces.toString();
  }

  /**
   * Determines whether the specified String value is blank, which is true if it is null, an empty String or a String
   * containing only spaces (blanks).
   * </p>
   * @param value the String value used in the determination for the "blank" check.
   * @return a boolean value indicating whether the specified String is blank.
   * @see #isEmpty(String)
   */
  public static boolean isBlank(final String value) {
    return (value == null || EMPTY_STRING.equals(value.trim()));
  }

  /**
   * Determines whether the specified String value is empty, which is true if and only if the value is the empty String.
   * </p>
   * @param value the String value used in the determination of the "empty" check.
   * @return a boolean value indicating if the specified String is empty.
   * @see #isBlank(String)
   */
  public static boolean isEmpty(final String value) {
    return EMPTY_STRING.equals(value);
  }

  /**
   * Pads the specified String value by appending the specified character up to the given length.
   * </p>
   * @param value the String value to pad by appending 'paddingCharacter' to the end.
   * @param paddingCharacter the character used to pad the end of the String value.
   * @param length an int value indicating the final length of the String value with padding of the 'paddingCharacter'.
   * @return the String value padded with the specified character by appending 'paddingCharacter' to the end of the
   * String value up to the given length.
   * @throws NullPointerException if the String value is null.
   */
  public static String padEnding(final String value, final char paddingCharacter, final int length) {
    if (value == null) {
      throw new NullPointerException("The String value to pad cannot be null!");
    }

    final StringBuilder buffer = new StringBuilder(value);

    for (int valueLength = value.length(); valueLength < length; valueLength++) {
      buffer.append(paddingCharacter);
    }

    return buffer.toString();
  }

  /**
   * A null-safe implementation of the String.toLowerCase method.
   * </p>
   * @param value a String value to convert to lower case.
   * @return a lower case representation of the specified String value.
   * @see java.lang.String#toLowerCase()
   */
  public static String toLowerCase(final String value) {
    return (value == null ? null : value.toLowerCase());
  }

  /**
   * A null-safe implementation of the String.toUpperCase method.
   * </p>
   * @param value a String value to convert to upper case.
   * @return an upper case representation of the specified String value.
   * @see java.lang.String#toUpperCase()
   */
  public static String toUpperCase(final String value) {
    return (value == null ? null : value.toUpperCase());
  }

  /**
   * A method to trim the value of a String and guard against null values.
   * <p/>
   * @param value the String value that will be trimmed if not null.
   * @return null if the String value is null or the trimmed version of the String value if String value is not null.
   * @see java.lang.String#trim()
   */
  public static String trim(final String value) {
    return (value == null ? null : value.trim());
  }

  /**
   * Null-safe implementation of String truncate using substring.  Truncates the specified String value to the specified
   * length.  Returns null if the String value is null.
   * </p>
   * @param value the String value to truncate.
   * @param length an int value indicating the length to truncate the String value to.
   * @return the String value truncated to specified length, or null if the String value is null.
   * @throws IllegalArgumentException if the value of length is less than 0.
   * @see java.lang.String#substring(int, int)
   */
  public static String truncate(final String value, final int length) {
    if (length < 0) {
      throw new IllegalArgumentException("Length must be greater than equal to 0!");
    }

    return (value == null ? null : value.substring(0, Math.min(value.length(), length)));
  }

  /**
   * Gets the value of the specified Object as a String.  If the Object is null then the first non-null String value
   * from the array of default String value is returned.  If the array of String values is null or all the elements
   * in the default String values array are null, then the value of String.valueOf(value) is returned.
   * </p>
   * @param value the Object who's String representation is being evaluated.
   * @param defaultValues an array of default String values to assess if the specified Object value is null.
   * @return a String representation of the specified Object value or one of the default String values from the array
   * if the Object value is null.  If either the default String array is null or all the elements are null, then
   * the String value of String.valueOf(value) is returned.
   * @see java.lang.String#valueOf(Object)
   */
  public static String valueOf(final Object value, final String... defaultValues) {
    if (value != null) {
      return value.toString();
    }
    else {
      if (defaultValues != null) {
        for (String defaultValue : defaultValues) {
          if (defaultValue != null) {
            return defaultValue;
          }
        }
      }

      return String.valueOf(value);
    }
  }

  /**
   * Wraps a line of text to no longer than the specified width, measured by the number of characters in each line,
   * indenting all subsequent lines with the indent.  If the indent is null, then an empty String is used.
   * </p>
   * @param line a String containing the line of text to wrap.
   * @param widthInCharacters an integer value indicating the width of each line measured by the number of characters.
   * @param indent the String value used to indent all subsequent lines.
   * @return the line of text wrapped.
   * @throws IndexOutOfBoundsException if widthInCharacters is less than 0, or there are no word boundaries within
   * the given width on any given split.
   * @throws NullPointerException if the line of text is null.
   */
  public static String wrap(String line, final int widthInCharacters, String indent) {
    final StringBuilder buffer = new StringBuilder();

    int lineCount = 1;
    int spaceIndex = -1;

    // if indent is null, then do not indent the wrapped lines
    indent = valueOf(indent, EMPTY_STRING);

    while (line.length() > widthInCharacters) {
      spaceIndex = line.substring(0, widthInCharacters).lastIndexOf(SPACE);
      buffer.append(lineCount++ > 1 ? indent : EMPTY_STRING);
      // throws IndexOutOfBoundsException if spaceIndex is -1, implying no word boundary was found within
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

  private static final int MAX_ARRAY_ELEMENTS_TO_CONVERT = Integer.getInteger("StringUtils.MAX_ARRAY_ELEMENTS_TO_CONVERT", 16);
  
  /**
   * Used to convert the given object to a String. If anything goes wrong in this conversion
   * put some info about what went wrong on the result string but do not throw an exception.
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
   * Convert an object to a string and return it.
   * Handled CacheDeserializables without having them change the form they store.
   * If deserialization is needed and fails then the string contains a message saying so instead of throwing an exception.
   * @param o the object to convert to a string
   * @param convertArrayContents if true then the contents of the array will be in the string; otherwise just the array identity
   * @param maxArrayElements if convertArrayContents is true then this parameter limits how many array elements are converted to the string.
   *                         After the last converted element "and NNN more" is used to indicate the number of elements not converted.
   */
  public static String objectToString(Object o, boolean convertArrayContents, int maxArrayElements) {
    if (o == null || o == Token.NOT_AVAILABLE) {
      return "null";
    } else if (o instanceof CachedDeserializable) {
      CachedDeserializable cd = (CachedDeserializable)o;
      return cd.getStringForm();
    } else if (convertArrayContents && o.getClass().isArray()) {
      Class<?> eClass = o.getClass();
      if (eClass == byte[].class) {
        return arrayToString((byte[])o, maxArrayElements);
      } else if (eClass == boolean[].class) {
        return arrayToString((boolean[])o, maxArrayElements);
      } else if (eClass == char[].class) {
        return arrayToString((char[])o, maxArrayElements);
      } else if (eClass == short[].class) {
        return arrayToString((short[])o, maxArrayElements);
      } else if (eClass == int[].class) {
        return arrayToString((int[])o, maxArrayElements);
      } else if (eClass == long[].class) {
        return arrayToString((long[])o, maxArrayElements);
      } else if (eClass == float[].class) {
        return arrayToString((float[])o, maxArrayElements);
      } else if (eClass == double[].class) {
        return arrayToString((double[])o, maxArrayElements);
      } else {
        return arrayToString((Object[]) o, maxArrayElements);
      }
    } else {
      return o.toString();
    }
  }
  
  private static String arrayToString(Object[] a, int maxArrayElements) {
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
    Class componentType = a.getClass().getComponentType();
    if (iMax == -1) {
      return componentType.getSimpleName() + "[]";
    }
    StringBuilder b = new StringBuilder();
    b.append(componentType.getSimpleName());
    b.append('[');
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
    for (int i = 0; ; i++) {
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
