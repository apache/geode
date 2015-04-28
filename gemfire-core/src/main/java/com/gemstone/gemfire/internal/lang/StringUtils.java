/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

/**
 * The StringUtils is an abstract utility class for working with and invoking operations on String literals.
 * <p/>
 * @author John J. Blum
 * @see java.lang.String
 * @since 7.0
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

  /**
   * Used to convert the given object to a String. If anything goes wrong in this conversion
   * put some info about what went wrong on the result string but do not throw an exception.
   * @param o the object to convert to a string
   * @return the string from of the given object.
   */
  public static String forceToString(Object o) {
    try {
      return o.toString();
    } catch (RuntimeException ex) {
      return "Conversion to a string failed because " + ex;
    }
  }

}
