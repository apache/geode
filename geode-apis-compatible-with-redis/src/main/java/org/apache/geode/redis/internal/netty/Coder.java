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
 *
 */
package org.apache.geode.redis.internal.netty;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.ARRAY_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.BULK_STRING_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.ERROR_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.INTEGER_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.NUMBER_0_BYTE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.N_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.P_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.SIMPLE_STRING_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bBUSYKEY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCRLF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bEMPTY_ARRAY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bEMPTY_STRING;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bERR;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINTEGER_MIN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLONG_MIN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_A;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_E;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_Z;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMINUS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMOVED;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNEGATIVE_ZERO;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNIL;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNaN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOK;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOOM;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPERIOD;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPLUS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPOSITIVE_ZERO;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bUPPERCASE_E;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWRONGTYPE;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.bytes.ByteArrays;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.redis.internal.data.RedisKey;

/**
 * This is a safe encoder and decoder for all redis matching needs
 */
public class Coder {

  /**
   * The charset being used by this coder, {@value #CHARSET}.
   */
  public static final String CHARSET = "UTF-8";

  // In UTF-8 encoding, upper- and lowercase versions of the same character differ by this amount
  public static final int CHARSET_CASE_OFFSET = 32;

  @MakeImmutable
  protected static final DecimalFormat decimalFormatter = new DecimalFormat("#");

  static {
    decimalFormatter.setMaximumFractionDigits(10);
  }

  public static ByteBuf getBulkStringResponse(ByteBuf buffer, Object v)
      throws CoderException {
    byte[] toWrite;

    if (v == null) {
      buffer.writeBytes(bNIL);
    } else if (v instanceof byte[]) {
      toWrite = (byte[]) v;
      writeStringResponse(buffer, toWrite);
    } else if (v instanceof RedisKey) {
      toWrite = ((RedisKey) v).toBytes();
      writeStringResponse(buffer, toWrite);
    } else if (v instanceof Double) {
      toWrite = doubleToBytes((Double) v);
      writeStringResponse(buffer, toWrite);
    } else if (v instanceof String) {
      String value = (String) v;
      toWrite = stringToBytes(value);
      writeStringResponse(buffer, toWrite);
    } else if (v instanceof Integer) {
      getIntegerResponse(buffer, (int) v);
    } else if (v instanceof Long) {
      getIntegerResponse(buffer, (long) v);
    } else {
      throw new CoderException();
    }

    return buffer;
  }

  private static void writeStringResponse(ByteBuf buffer, byte[] toWrite) {
    buffer.writeByte(BULK_STRING_ID);
    appendAsciiDigitsToByteBuf(toWrite.length, buffer);
    buffer.writeBytes(bCRLF);
    buffer.writeBytes(toWrite);
    buffer.writeBytes(bCRLF);
  }

  public static ByteBuf getFlattenedArrayResponse(ByteBuf buffer, Collection<Collection<?>> items)
      throws CoderException {
    for (Object next : items) {
      writeCollectionOrString(buffer, next);
    }

    return buffer;
  }

  public static ByteBuf getArrayResponse(ByteBuf buffer, Collection<?> items)
      throws CoderException {
    buffer.writeByte(ARRAY_ID);
    appendAsciiDigitsToByteBuf(items.size(), buffer);
    buffer.writeBytes(bCRLF);
    for (Object next : items) {
      writeCollectionOrString(buffer, next);
    }

    return buffer;
  }

  private static void writeCollectionOrString(ByteBuf buffer, Object next) throws CoderException {
    if (next instanceof Collection) {
      Collection<?> nextItems = (Collection<?>) next;
      getArrayResponse(buffer, nextItems);
    } else {
      getBulkStringResponse(buffer, next);
    }
  }

  public static ByteBuf getScanResponse(ByteBuf buffer, BigInteger cursor,
      List<?> scanResult) {
    buffer.writeByte(ARRAY_ID);
    buffer.writeByte(digitToAscii(2));
    buffer.writeBytes(bCRLF);
    byte[] cursorBytes = stringToBytes(cursor.toString());
    writeStringResponse(buffer, cursorBytes);
    buffer.writeByte(ARRAY_ID);
    appendAsciiDigitsToByteBuf(scanResult.size(), buffer);
    buffer.writeBytes(bCRLF);

    for (Object nextObject : scanResult) {
      if (nextObject instanceof String) {
        String next = (String) nextObject;
        writeStringResponse(buffer, stringToBytes(next));
      } else if (nextObject instanceof RedisKey) {
        writeStringResponse(buffer, ((RedisKey) nextObject).toBytes());
      } else {
        writeStringResponse(buffer, (byte[]) nextObject);
      }
    }
    return buffer;
  }

  public static ByteBuf getEmptyArrayResponse(ByteBuf buffer) {
    buffer.writeBytes(bEMPTY_ARRAY);
    return buffer;
  }

  public static ByteBuf getEmptyStringResponse(ByteBuf buffer) {
    buffer.writeBytes(bEMPTY_STRING);
    return buffer;
  }

  public static ByteBuf getSimpleStringResponse(ByteBuf buffer, String string) {
    byte[] simpAr = stringToBytes(string);
    return getSimpleStringResponse(buffer, simpAr);
  }

  public static ByteBuf getSimpleStringResponse(ByteBuf buffer, byte[] byteArray) {
    buffer.writeByte(SIMPLE_STRING_ID);
    buffer.writeBytes(byteArray);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  private static ByteBuf getErrorResponse0(ByteBuf buffer, byte[] type, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(type);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getErrorResponse(ByteBuf buffer, String error) {
    return getErrorResponse0(buffer, bERR, error);
  }

  public static ByteBuf getMovedResponse(ByteBuf buffer, String error) {
    return getErrorResponse0(buffer, bMOVED, error);
  }

  public static ByteBuf getOOMResponse(ByteBuf buffer, String error) {
    return getErrorResponse0(buffer, bOOM, error);
  }

  public static ByteBuf getWrongTypeResponse(ByteBuf buffer, String error) {
    return getErrorResponse0(buffer, bWRONGTYPE, error);
  }

  public static ByteBuf getBusyKeyResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(bBUSYKEY);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getCustomErrorResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, int integer) {
    buffer.writeByte(INTEGER_ID);
    appendAsciiDigitsToByteBuf(integer, buffer);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, long l) {
    buffer.writeByte(INTEGER_ID);
    appendAsciiDigitsToByteBuf(l, buffer);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getBigDecimalResponse(ByteBuf buffer, BigDecimal b) {
    writeStringResponse(buffer, bigDecimalToBytes(b));
    return buffer;
  }

  public static ByteBuf getOKResponse(ByteBuf buffer) {
    buffer.writeBytes(bOK);
    return buffer;
  }

  public static ByteBuf getNilResponse(ByteBuf buffer) {
    buffer.writeBytes(bNIL);
    return buffer;
  }

  public static String bytesToString(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      return new String(bytes, CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String doubleToString(double d) {
    if (d == Double.POSITIVE_INFINITY) {
      return "inf";
    }
    if (d == Double.NEGATIVE_INFINITY) {
      return "-inf";
    }

    String stringValue = String.valueOf(d);
    if (stringValue.endsWith(".0")) {
      return (stringValue.substring(0, stringValue.length() - 2));
    }
    return stringValue;
  }

  public static byte[] stringToBytes(String string) {
    if (string == null) {
      return null;
    }
    try {
      return string.getBytes(CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  /*
   * These toByte methods convert to byte arrays of the string representation of the input, not
   * literal to byte
   */

  /**
   * NOTE: Canonical byte arrays may be returned so callers
   * must never modify the returned array.
   */
  public static byte[] intToBytes(int i) {
    return longToBytes(i);
  }

  /**
   * NOTE: Canonical byte arrays may be returned so callers
   * must never modify the returned array.
   */
  public static byte[] longToBytes(long l) {
    return convertLongToAsciiDigits(l);
  }

  public static byte[] doubleToBytes(double d) {
    return convertDoubleToAsciiBytes(d);
    //return stringToBytes(doubleToString(d));
  }

  public static byte[] bigDecimalToBytes(BigDecimal b) {
    return stringToBytes(b.toPlainString());
  }

  public static BigDecimal bytesToBigDecimal(byte[] bytes) {
    return new BigDecimal(bytesToString(bytes));
  }

  public static long bytesToLong(byte[] bytes) {
    return parseLong(bytes);
  }

  private static NumberFormatException createNumberFormatException(byte[] bytes) {
    return new NumberFormatException("For input string: \"" + bytesToString(bytes) + "\"");
  }

  /**
   * This method was derived from openjdk Long.java parseLong
   */
  public static long parseLong(byte[] bytes) throws NumberFormatException {
    final int len = bytes.length;
    if (len <= 0) {
      throw createNumberFormatException(bytes);
    }
    int i = 0;
    long limit = -Long.MAX_VALUE;
    boolean negative = false;
    byte firstByte = bytes[0];
    if (firstByte < NUMBER_0_BYTE) { // Possible leading "+" or "-"
      if (firstByte == bMINUS) {
        negative = true;
        limit = Long.MIN_VALUE;
      } else if (firstByte != bPLUS) {
        throw createNumberFormatException(bytes);
      }
      if (len == 1) { // Cannot have lone "+" or "-"
        throw createNumberFormatException(bytes);
      }
      i++;
    }
    final long multmin = limit / 10;
    long result = 0;
    while (i < len) {
      // Accumulating negatively avoids surprises near MAX_VALUE
      int digit = asciiToDigit(bytes[i++]);
      if (digit < 0 || result < multmin) {
        throw createNumberFormatException(bytes);
      }
      result *= 10;
      if (result < limit + digit) {
        throw createNumberFormatException(bytes);
      }
      result -= digit;
    }
    return negative ? result : -result;
  }


  /**
   * A conversion where the byte array actually represents a string, so it is converted as a string
   * not as a literal double
   *
   * @param bytes Array holding double
   * @return Parsed value
   * @throws NumberFormatException if bytes to string does not yield a convertible double
   */
  public static double bytesToDouble(byte[] bytes) {
    return parseDouble(bytes);
  }

  /**
   * Redis specific manner to parse floats
   *
   * @param d String holding double
   * @return Value of string
   * @throws NumberFormatException if the double cannot be parsed
   */
  private static double stringToDouble(String d) {
    if (d.equalsIgnoreCase(P_INF)) {
      return Double.POSITIVE_INFINITY;
    } else if (d.equalsIgnoreCase(N_INF)) {
      return Double.NEGATIVE_INFINITY;
    } else {
      return Double.parseDouble(d);
    }
  }

  private static final int INT_MAX_DIV10 = Integer.MAX_VALUE / 10;
  private static final long LONG_MAX_DIV10 = Long.MAX_VALUE / 10;

  /**
   * Derived from javolution.text.TypeFormat.parseDouble.
   * Improved overflow detection in decimal value
   * with conversion to exponent (99999999999999.99999 is handled).
   */
  static double parseDouble(final byte[] bytes) throws NumberFormatException {

    if (isPositiveInfinity(bytes)) {
      return POSITIVE_INFINITY;
    }
    if (isNegativeInfinity(bytes)) {
      return NEGATIVE_INFINITY;
    }
    if (isNaN(bytes)) {
      return NaN;
    }
    if (false) // use jdk
      return stringToDouble(bytesToString(bytes));

    final int offset = 0;
    final int end = bytes.length;
    int i = offset;
    byte b = bytes[i];
    // Reads sign.
    boolean isNegative = (b == bMINUS);
    if ((isNegative || (b == bPLUS)) && (++i < end)) {
      b = bytes[i];
    }

    int digit = asciiToDigit(b);
    if (digit == -1 && (b != bPERIOD)) {
      throw new NumberFormatException("Digit or '.' required");
    }

    // Reads decimal and fraction (both merged to a long).
    long decimal = 0;
    int decimalPoint = -1;
    int decimalExp = 0;
    while (true) {
      if (digit >= 0) {
        long tmp = decimal * 10 + digit;
        if ((decimal > LONG_MAX_DIV10) || (tmp < decimal)) {
          decimalExp++;
        } else {
          decimal = tmp;
        }
      } else if ((b == bPERIOD) && (decimalPoint < 0)) {
        decimalPoint = i;
      } else {
        break;
      }
      if (++i >= end) {
        break;
      }
      b = bytes[i];
      digit = asciiToDigit(b);
    }
    if (isNegative) {
      decimal = -decimal;
    }
    int fractionLength = (decimalPoint >= 0) ? i - decimalPoint - 1 : 0;

    // Reads exponent.
    int exp = 0;
    if ((i < end) && ((b == bUPPERCASE_E) || (b == bLOWERCASE_E))) {
      b = bytes[++i];
      boolean isNegativeExp = (b == bMINUS);
      if ((isNegativeExp || (b == bPLUS)) && (++i < end)) {
        b = bytes[i];
      }
      digit = asciiToDigit(b);
      while (digit != -1) {
        int tmp = exp * 10 + digit;
        if ((exp > INT_MAX_DIV10) || (tmp < exp))
          throw new NumberFormatException("Exponent Overflow");
        exp = tmp;
        if (++i >= end) {
          break;
        }
        b = bytes[i];
        digit = asciiToDigit(b);
      }
      if (digit == -1) {
        throw new NumberFormatException("Invalid exponent");
      }
      if (isNegativeExp) {
        exp = -exp;
      }
    }

    if (i < end) {
      throw new NumberFormatException("Invalid characters in input");
    }

    exp += decimalExp;

    return toDoublePow10(decimal, exp - fractionLength);
  }


  /**
   * This method allows comparison of byte array representations of Strings, ignoring case, allowing
   * the return of String.equalsIgnoreCase() to be evaluated without performing expensive String
   * conversion. This method should only be used to compare against known constant byte arrays found
   * in the {@link StringBytesGlossary} class.
   *
   * @param test the byte array representation of the String we wish to compare
   * @param expected a byte array constant from the {@link StringBytesGlossary} class representing
   *        the String we wish to compare against
   * @return true if the Strings represented by the byte arrays would return true from
   *         {@code test.equalsIgnoreCase(expected)}, false otherwise
   */
  public static boolean equalsIgnoreCaseBytes(byte[] test, byte[] expected) {
    if (test == expected) {
      return true;
    }
    if (test == null || expected == null || test.length != expected.length) {
      return false;
    }
    for (int i = 0; i < expected.length; ++i) {
      if (toUpperCase(test[i]) != toUpperCase(expected[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Transforms a byte array representing a String into a byte array representing the uppercase
   * version of that String.
   *
   * @param bytes the byte array representing a String to be transformed
   * @return a byte array representing the result of calling String.toUpperCase() on the input
   *         String
   */
  public static byte[] toUpperCaseBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    byte[] uppercase = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i) {
      uppercase[i] = toUpperCase(bytes[i]);
    }
    return uppercase;
  }

  private static byte toUpperCase(byte b) {
    return isLowerCase(b) ? (byte) (b - CHARSET_CASE_OFFSET) : b;
  }

  private static boolean isLowerCase(byte value) {
    return value >= bLOWERCASE_A && value <= bLOWERCASE_Z;
  }

  public static boolean isInfinity(byte[] bytes) {
    return isPositiveInfinity(bytes) || isNegativeInfinity(bytes);
  }

  // Checks if the given byte array is equivalent to the String "NaN", ignoring case.
  public static boolean isNaN(byte[] bytes) {
    return equalsIgnoreCaseBytes(bytes, bNaN);
  }

  // Checks if the given byte array is equivalent to the Strings "INF", "INFINITY", "+INF" or
  // "+INFINITY", ignoring case.
  public static boolean isPositiveInfinity(byte[] bytes) {
    return equalsIgnoreCaseBytes(bytes, bINF)
        || equalsIgnoreCaseBytes(bytes, bP_INF)
        || equalsIgnoreCaseBytes(bytes, bINFINITY)
        || equalsIgnoreCaseBytes(bytes, bP_INFINITY);
  }

  // Checks if the given byte array is equivalent to the Strings "-INF" or "-INFINITY", ignoring
  // case.
  public static boolean isNegativeInfinity(byte[] bytes) {
    return equalsIgnoreCaseBytes(bytes, bN_INF)
        || equalsIgnoreCaseBytes(bytes, bN_INFINITY);
  }

  // Takes a long value and converts it to an int. Values outside the range Integer.MIN_VALUE and
  // Integer.MAX_VALUE will be converted to either Integer.MIN_VALUE or Integer.MAX_VALUE
  public static int narrowLongToInt(long toBeNarrowed) {
    if (toBeNarrowed > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else if (toBeNarrowed < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    } else {
      return (int) toBeNarrowed;
    }
  }

  public static byte[] stripTrailingZeroFromDouble(byte[] doubleBytes) {
    if (doubleBytes.length > 1 && doubleBytes[doubleBytes.length - 2] == bPERIOD
        && doubleBytes[doubleBytes.length - 1] == NUMBER_0_BYTE) {
      return Arrays.copyOfRange(doubleBytes, 0, doubleBytes.length - 2);
    }
    return doubleBytes;
  }

  /**
   * Takes the given "value" and computes the sequence of ASCII digits it represents,
   * appending them to the given "buf".
   */
  public static void appendAsciiDigitsToByteBuf(long value, ByteBuf buf) {
    buf.writeBytes(convertLongToAsciiDigits(value));
  }

  /**
   * Convert the given "value" to a sequence of ASCII digits and
   * returns them in a byte array. The only byte in the array that
   * may not be a digit is the first byte will be '-' for a negative value.
   * NOTE: the returned array's contents must not be modified by the caller.
   */
  private static byte[] convertLongToAsciiDigits(long value) {
    final boolean negative = value < 0;
    if (!negative) {
      value = -value;
    }
    // at this point value <= 0

    if (value > -100) {
      // it has at most two digits: [-99..0]
      return convertSmallLongToAsciiDigits((int) value, negative);
    } else {
      return convertBigLongToAsciiDigits(value, negative);
    }
  }

  /**
   * value is in the range [-99..0].
   * This could be done using computation but a simple
   * table lookup allows no allocations to be done since
   * a canonical instance is returned.
   */
  private static byte[] convertSmallLongToAsciiDigits(int value, boolean negative) {
    value = -value;
    // value is now 0..99
    if (negative) {
      return NEGATIVE_TABLE[value];
    } else {
      return POSITIVE_TABLE[value];
    }
  }

  private static final int TABLE_SIZE = 100;
  @Immutable
  private static final byte[][] NEGATIVE_TABLE = new byte[TABLE_SIZE][];
  @Immutable
  private static final byte[][] POSITIVE_TABLE = new byte[TABLE_SIZE][];
  static {
    for (int i = 0; i < TABLE_SIZE; i++) {
      NEGATIVE_TABLE[i] = createTwoDigitArray(-i, true);
      POSITIVE_TABLE[i] = createTwoDigitArray(-i, false);
    }
  }

  private static byte[] createTwoDigitArray(int value, boolean negative) {
    int quotient = value / 10;
    int remainder = (quotient * 10) - value;
    int resultSize = (quotient < 0) ? 2 : 1;
    if (negative) {
      resultSize++;
    }
    byte[] result = new byte[resultSize];
    int resultIdx = 0;
    if (negative) {
      result[resultIdx++] = bMINUS;
    }
    if (quotient < 0) {
      result[resultIdx++] = digitToAscii(-quotient);
    }
    result[resultIdx] = digitToAscii(remainder);
    return result;
  }

  /**
   * This code was adapted from the openjdk Long.java getChars methods.
   */
  private static byte[] convertBigLongToAsciiDigits(long value, boolean negative) {
    final byte[] bytes = new byte[asciiByteLength(value, negative)];
    int bytePos = bytes.length;

    long quotient;
    int remainder;
    // Get 2 digits/iteration using longs until quotient fits into an int
    while (value <= Integer.MIN_VALUE) {
      quotient = value / 100;
      remainder = (int) ((quotient * 100) - value);
      value = quotient;
      bytes[--bytePos] = DIGIT_ONES[remainder];
      bytes[--bytePos] = DIGIT_TENS[remainder];
    }

    // Get 2 digits/iteration using ints
    int intQuotient;
    int intValue = (int) value;
    while (intValue <= -100) {
      intQuotient = intValue / 100;
      remainder = (intQuotient * 100) - intValue;
      intValue = intQuotient;
      bytes[--bytePos] = DIGIT_ONES[remainder];
      bytes[--bytePos] = DIGIT_TENS[remainder];
    }

    // We know there are at most two digits left at this point.
    intQuotient = intValue / 10;
    remainder = (intQuotient * 10) - intValue;
    bytes[--bytePos] = digitToAscii(remainder);

    // Whatever left is the remaining digit.
    if (intQuotient < 0) {
      bytes[--bytePos] = digitToAscii(-intQuotient);
    }
    if (negative) {
      bytes[--bytePos] = bMINUS;
    }
    return bytes;
  }

  /**
   * This code was derived from openjdk Long.java stringSize
   * Returns the number of bytes needed to represent "value" as ascii bytes
   * Note that "value" has already been negated if it was positive
   * and we are told if that happened with the "negative" parameter.
   *
   * @param value long value already normalized to be <= 0.
   * @param negative true if the original "value" was < 0.
   * @return number of bytes needed to represent "value" as ascii bytes
   *
   * @implNote There are other ways to compute this: e.g. binary search,
   *           but values are biased heavily towards zero, and therefore linear search
   *           wins. The iteration results are also routinely inlined in the generated
   *           code after loop unrolling.
   */
  private static int asciiByteLength(long value, boolean negative) {
    final int nonDigitCount = negative ? 1 : 0;
    // Note since this is only called if value >= -100
    // (see the caller of convertSmallLongToAsciiDigits)
    // we skip the first two loops by starting
    // powerOf10 at -1000 (instead of -10)
    // and digitCount at 3 (instead of 1).
    long powerOf10 = -1000;
    final int MAX_DIGITS = 19;
    for (int digitCount = 3; digitCount < MAX_DIGITS; digitCount++) {
      if (value > powerOf10) {
        return digitCount + nonDigitCount;
      }
      powerOf10 *= 10;
    }
    return MAX_DIGITS + nonDigitCount;
  }

  /**
   * Given a digit (a value in the range 0..9) return its corresponding ASCII code.
   * NOTE: the caller is responsible for ensuring that 'digit' is in the correct range.
   */
  public static byte digitToAscii(int digit) {
    return (byte) (NUMBER_0_BYTE + digit);
  }

  public static int asciiToDigit(byte ascii) {
    if (ascii >= NUMBER_0_BYTE && ascii <= NUMBER_0_BYTE + 9) {
      return ascii - NUMBER_0_BYTE;
    }
    return -1;
  }

  @Immutable
  private static final byte[] DIGIT_TENS = new byte[10 * 10];
  static {
    for (byte i = 0; i < 10; i++) {
      for (byte j = 0; j < 10; j++) {
        DIGIT_TENS[(i * 10) + j] = digitToAscii(i);
      }
    }
  }

  @Immutable
  private static final byte[] DIGIT_ONES = new byte[10 * 10];
  static {
    for (byte i = 0; i < 10; i++) {
      for (byte j = 0; j < 10; j++) {
        DIGIT_ONES[(i * 10) + j] = digitToAscii(j);
      }
    }
  }

  private static final long TEN_TO_7TH = (long)Math.pow(10, 7);
  private static final long TEN_TO_MINUS_3RD = (long)Math.pow(10, -3);


  /**
   * @param d The double precision value to convert.
   */
  private static byte[] convertDoubleToAsciiBytes(double d) {
    if (d != d) { // NaN
      return bNaN;
    }
    if (d == Double.POSITIVE_INFINITY) {
      return bINF;
    }
    if (d == Double.NEGATIVE_INFINITY) {
      return bN_INF;
    }
    if (d == 0.0) { // Zero.
      return bPOSITIVE_ZERO;
    }
    final int MAX_RESULT_SIZE = 26;
    int resultPos = 0;
    byte[] result = new byte[MAX_RESULT_SIZE];
    if (d < 0) { // Work with positive number.
      d = -d;
      result[resultPos++] = bMINUS;
    }

    // Find the exponent e such as: value == x.xxx * 10^e
    int e = floorLog10(d);

    long m;
    final int digits; // 16 or 17 digits
    // Try 17 digits.
    long m17 = toLongPow10(d, (17 - 1) - e);
    // Check if we can use 16 digits.
    long m16 = m17 / 10;
    double dd = toDoublePow10(m16, e - 16 + 1);
    if (dd == d) { // 16 digits is enough.
      digits = 16;
      m = m16;
    } else { // We cannot remove the last digit.
      digits = 17;
      m = m17;
    }

    // Formats.
    if (e >= digits) {
      // Scientific notation has to be used ("x.xxxEyy").
      long pow10 = POW10_LONG[digits - 1];
      int k = (int) (m / pow10); // Single digit.
      resultPos = appendDigit(result, resultPos, k);
      m = m - pow10 * k;
      resultPos = appendFraction(result, resultPos, m, digits - 1, true);
      result[resultPos++] = bUPPERCASE_E;
      resultPos = append(result, resultPos, e);
    } else { // Dot within the string ("xxxx.xxxxx").
      int exp = digits - e - 1;
      if (exp < POW10_LONG.length) {
        long pow10 = POW10_LONG[exp];
        long l = m / pow10;
        resultPos = append(result, resultPos, l);
        m = m - pow10 * l;
      } else {
        // Result of the division by a power of 10 larger than any long.
        resultPos = appendDigit(result, resultPos, 0);
      }
      resultPos = appendFraction(result, resultPos, m, exp, false);
    }
    return ByteArrays.copy(result, 0, resultPos);
  }

  /**
   * digit must be in the range [0..9]
   */
  private static int appendDigit(byte[] result, int resultPos, int digit) {
    result[resultPos++] = digitToAscii(digit);
    return resultPos;
  }

  /**
   * Append "count" copies of "digit" to the byte array.
   * digit must be in the range [0..9]
   */
  private static int appendDigits(byte[] result, int resultPos, int digit, int count) {
    while (count > 0) {
      resultPos = appendDigit(result, resultPos, digit); // Add leading zeros.
      count--;
    }
    return resultPos;
  }

  private static int append(byte[] result, int resultPos, byte[] src) {
    System.arraycopy(src, 0, result, resultPos, src.length);
    resultPos += src.length;
    return resultPos;
  }

  /**
   * Appends the decimal representation of the specified <code>int</code>
   * argument.
   *
   * @param result the byte array to append to
   * @param resultPos the index of the first byte in the array to target
   * @param i the <code>int</code> to format.
   * @return returns updated resultPos
   */
  private static int append(byte[] result, int resultPos, int i) {
    if (i <= 0) {
      if (i == 0) {
        return appendDigit(result, resultPos, 0);
      }
      if (i == Integer.MIN_VALUE) {// Negation would overflow.
        return append(result, resultPos, bINTEGER_MIN);
      }
      result[resultPos++] = bMINUS;
      i = -i;
    }
    int digits = digitLength(i);
    resultPos += digits;
    for (int index = resultPos - 1;; index--) {
      int j = i / 10;
      result[index] = digitToAscii(i - (j * 10));
      if (j == 0) {
        return resultPos;
      }
      i = j;
    }
  }

  /**
   * Appends the decimal representation of the specified <code>int</code>
   * argument.
   *
   * @param result the byte array to append to
   * @param resultPos the index of the first byte in the array to target
   * @param l the <code>long</code> to format.
   * @return returns updated resultPos
   */
  private static int append(byte[] result, int resultPos, long l) {
    if (l <= 0) {
      if (l == 0) {
        return appendDigit(result, resultPos, 0);
      }
      if (l == Long.MIN_VALUE) {// Negation would overflow.
        return append(result, resultPos, bLONG_MIN);
      }
      result[resultPos++] = bMINUS;
      l = -l;
    }
    if (l <= Integer.MAX_VALUE) {
      return append(result, resultPos, (int) l);
    }
    resultPos = append(result, resultPos, l / 1000000000);
    int i = (int) (l % 1000000000);
    int digits = digitLength(i);
    resultPos = appendDigits(result, resultPos, 0, 9 - digits);
    return append(result, resultPos, i);
  }


  private static int appendFraction(byte[] result, int resultPos, long l, int digits,
      boolean forceDotZero) {
    if (l == 0) {
      if (forceDotZero) {
        result[resultPos++] = bPERIOD;
        resultPos = appendDigit(result, resultPos, 0);
      }
    } else { // l is different from zero.
      result[resultPos++] = bPERIOD;
      int length = digitLength(l);
      resultPos = appendDigits(result, resultPos, 0, digits - length); // Add leading zeros.
      while (l % 10 == 0) {
        l /= 10; // Remove trailing zeros.
      }
      resultPos = append(result, resultPos, l);
    }
    return resultPos;
  }

  /**
   * Returns the number of digits of the decimal representation of the specified <code>int</code>
   * value,
   * excluding the sign character if any.
   *
   * @param i the <code>int</code> value for which the digit length is returned.
   * @return <code>String.valueOf(i).length()</code> for zero or positive values;
   *         <code>String.valueOf(i).length() - 1</code> for negative values.
   */
  private static int digitLength(int i) {
    if (i >= 0) {
      return (i >= 100000) ? (i >= 10000000) ? (i >= 1000000000) ? 10
          : (i >= 100000000) ? 9 : 8 : (i >= 1000000) ? 7 : 6
          : (i >= 100) ? (i >= 10000) ? 5 : (i >= 1000) ? 4 : 3
              : (i >= 10) ? 2 : 1;
    }
    if (i == Integer.MIN_VALUE) {
      return 10; // "2147483648".length()
    }
    return digitLength(-i); // No overflow possible.
  }

  /**
   * Returns the number of digits of the decimal representation of the the specified
   * <code>long</code>,
   * excluding the sign character if any.
   *
   * @param l the <code>long</code> value for which the digit length is returned.
   * @return <code>String.valueOf(l).length()</code> for zero or positive values;
   *         <code>String.valueOf(l).length() - 1</code> for negative values.
   */
  private static int digitLength(long l) {
    if (l >= 0) {
      return (l <= Integer.MAX_VALUE) ? digitLength((int) l)
          : // At least 10 digits or more.
          (l >= 100000000000000L) ? (l >= 10000000000000000L) ? (l >= 1000000000000000000L) ? 19
              : (l >= 100000000000000000L) ? 18 : 17
              : (l >= 1000000000000000L) ? 16 : 15
              : (l >= 100000000000L) ? (l >= 10000000000000L) ? 14
                  : (l >= 1000000000000L) ? 13 : 12
                  : (l >= 10000000000L) ? 11 : 10;
    }
    if (l == Long.MIN_VALUE) {
      return 19; // "9223372036854775808".length()
    }
    return digitLength(-l);
  }


  private static final long[] POW10_LONG = new long[] {1L, 10L, 100L, 1000L,
      10000L, 100000L, 1000000L, 10000000L, 100000000L, 1000000000L,
      10000000000L, 100000000000L, 1000000000000L, 10000000000000L,
      100000000000000L, 1000000000000000L, 10000000000000000L,
      100000000000000000L, 1000000000000000000L};

  /**
   * Returns the largest power of 10 that is less than or equal to the the specified positive value.
   *
   * @param d the <code>double</code> number.
   * @return <code>floor(Log10(abs(d)))</code>
   * @throws ArithmeticException if <code>d &lt;= 0</code> or <code>d</code>
   *         is <code>NaN</code> or <code>Infinity</code>.
   **/
  private static int floorLog10(double d) {
    int guess = (int) (LOG2_DIV_LOG10 * floorLog2(d));
    double pow10 = toDoublePow10(1, guess);
    if ((pow10 <= d) && (pow10 * 10 > d))
      return guess;
    if (pow10 > d)
      return guess - 1;
    return guess + 1;
  }

  /**
   * Returns the largest power of 2 that is less than or equal to the the specified positive value.
   *
   * @param d the <code>double</code> number.
   * @return <code>floor(Log2(abs(d)))</code>
   * @throws ArithmeticException if <code>d &lt;= 0</code> or <code>d</code>
   *         is <code>NaN</code> or <code>Infinity</code>.
   **/
  private static int floorLog2(double d) {
    if (d <= 0)
      throw new ArithmeticException("Negative number or zero");
    long bits = Double.doubleToLongBits(d);
    int exp = ((int) (bits >> 52)) & 0x7FF;
    if (exp == 0x7FF)
      throw new ArithmeticException("Infinity or NaN");
    if (exp == 0)
      return floorLog2(d * 18014398509481984L) - 54; // 2^54 Exact.
    return exp - 1023;
  }

  /**
   * Returns the closest <code>long</code> representation of the specified <code>double</code>
   * number multiplied
   * by a power of ten.
   *
   * @param d the <code>double</code> multiplier.
   * @param n the power of two exponent.
   * @return <code>d * 10<sup>n</sup></code>.
   */
  private static long toLongPow10(double d, int n) {
    long bits = Double.doubleToLongBits(d);
    boolean isNegative = (bits >> 63) != 0;
    int exp = ((int) (bits >> 52)) & 0x7FF;
    long m = bits & 0x000fffffffffffffL;
    if (exp == 0x7FF)
      throw new ArithmeticException(
          "Cannot convert to long (Infinity or NaN)");
    if (exp == 0) {
      if (m == 0)
        return 0L;
      return toLongPow10(d * 1E16, n - 16);
    }
    m |= 0x0010000000000000L; // Sets MSB (bit 52)
    int pow2 = exp - 1023 - 52;
    // Retrieves 63 bits m with n == 0.
    if (n >= 0) {
      // Works with 4 x 32 bits registers (x3:x2:x1:x0)
      long x0 = 0; // 32 bits.
      long x1 = 0; // 32 bits.
      long x2 = m & MASK_32; // 32 bits.
      long x3 = m >>> 32; // 32 bits.
      while (n != 0) {
        int i = (n >= POW5_INT.length) ? POW5_INT.length - 1 : n;
        int coef = POW5_INT[i]; // 31 bits max.

        if (((int) x0) != 0)
          x0 *= coef; // 63 bits max.
        if (((int) x1) != 0)
          x1 *= coef; // 63 bits max.
        x2 *= coef; // 63 bits max.
        x3 *= coef; // 63 bits max.

        x1 += x0 >>> 32;
        x0 &= MASK_32;

        x2 += x1 >>> 32;
        x1 &= MASK_32;

        x3 += x2 >>> 32;
        x2 &= MASK_32;

        // Adjusts powers.
        pow2 += i;
        n -= i;

        // Normalizes (x3 should be 32 bits max).
        long carry = x3 >>> 32;
        if (carry != 0) { // Shift.
          x0 = x1;
          x1 = x2;
          x2 = x3 & MASK_32;
          x3 = carry;
          pow2 += 32;
        }
      }

      // Merges registers to a 63 bits mantissa.
      int shift = 31 - bitLength(x3); // -1..30
      pow2 -= shift;
      m = (shift < 0) ? (x3 << 31) | (x2 >>> 1) : // x3 is 32 bits.
          (((x3 << 32) | x2) << shift) | (x1 >>> (32 - shift));

    } else { // n < 0

      // Works with x1:x0 126 bits register.
      long x1 = m; // 63 bits.
      long x0 = 0; // 63 bits.
      while (true) {

        // Normalizes x1:x0
        int shift = 63 - bitLength(x1);
        x1 <<= shift;
        x1 |= x0 >>> (63 - shift);
        x0 = (x0 << shift) & MASK_63;
        pow2 -= shift;

        // Checks if division has to be performed.
        if (n == 0)
          break; // Done.

        // Retrieves power of 5 divisor.
        int i = (-n >= POW5_INT.length) ? POW5_INT.length - 1 : -n;
        int divisor = POW5_INT[i];

        // Performs the division (126 bits by 31 bits).
        long wh = (x1 >>> 32);
        long qh = wh / divisor;
        long r = wh - qh * divisor;
        long wl = (r << 32) | (x1 & MASK_32);
        long ql = wl / divisor;
        r = wl - ql * divisor;
        x1 = (qh << 32) | ql;

        wh = (r << 31) | (x0 >>> 32);
        qh = wh / divisor;
        r = wh - qh * divisor;
        wl = (r << 32) | (x0 & MASK_32);
        ql = wl / divisor;
        x0 = (qh << 32) | ql;

        // Adjusts powers.
        n += i;
        pow2 -= i;
      }
      m = x1;
    }
    if (pow2 > 0)
      throw new ArithmeticException("Overflow");
    if (pow2 < -63)
      return 0;
    m = (m >> -pow2) + ((m >> -(pow2 + 1)) & 1); // Rounding.
    return isNegative ? -m : m;
  }

  /**
   * Returns the closest <code>double</code> representation of the specified <code>long</code>
   * number multiplied by
   * a power of ten.
   *
   * @param m the <code>long</code> multiplier.
   * @param n the power of ten exponent.
   * @return <code>multiplier * 10<sup>n</sup></code>.
   **/
  private static double toDoublePow10(long m, int n) {
    if (m == 0)
      return 0.0;
    if (m == Long.MIN_VALUE)
      return toDoublePow10(Long.MIN_VALUE / 10, n + 1);
    if (m < 0)
      return -toDoublePow10(-m, n);
    if (n >= 0) { // Positive power.
      if (n > 308)
        return Double.POSITIVE_INFINITY;
      // Works with 4 x 32 bits registers (x3:x2:x1:x0)
      long x0 = 0; // 32 bits.
      long x1 = 0; // 32 bits.
      long x2 = m & MASK_32; // 32 bits.
      long x3 = m >>> 32; // 32 bits.
      int pow2 = 0;
      while (n != 0) {
        int i = (n >= POW5_INT.length) ? POW5_INT.length - 1 : n;
        int coef = POW5_INT[i]; // 31 bits max.

        if (((int) x0) != 0)
          x0 *= coef; // 63 bits max.
        if (((int) x1) != 0)
          x1 *= coef; // 63 bits max.
        x2 *= coef; // 63 bits max.
        x3 *= coef; // 63 bits max.

        x1 += x0 >>> 32;
        x0 &= MASK_32;

        x2 += x1 >>> 32;
        x1 &= MASK_32;

        x3 += x2 >>> 32;
        x2 &= MASK_32;

        // Adjusts powers.
        pow2 += i;
        n -= i;

        // Normalizes (x3 should be 32 bits max).
        long carry = x3 >>> 32;
        if (carry != 0) { // Shift.
          x0 = x1;
          x1 = x2;
          x2 = x3 & MASK_32;
          x3 = carry;
          pow2 += 32;
        }
      }

      // Merges registers to a 63 bits mantissa.
      int shift = 31 - bitLength(x3); // -1..30
      pow2 -= shift;
      long mantissa = (shift < 0) ? (x3 << 31) | (x2 >>> 1) : // x3 is 32 bits.
          (((x3 << 32) | x2) << shift) | (x1 >>> (32 - shift));
      return toDoublePow2(mantissa, pow2);

    } else { // n < 0
      if (n < -324 - 20)
        return 0.0;

      // Works with x1:x0 126 bits register.
      long x1 = m; // 63 bits.
      long x0 = 0; // 63 bits.
      int pow2 = 0;
      while (true) {

        // Normalizes x1:x0
        int shift = 63 - bitLength(x1);
        x1 <<= shift;
        x1 |= x0 >>> (63 - shift);
        x0 = (x0 << shift) & MASK_63;
        pow2 -= shift;

        // Checks if division has to be performed.
        if (n == 0)
          break; // Done.

        // Retrieves power of 5 divisor.
        int i = (-n >= POW5_INT.length) ? POW5_INT.length - 1 : -n;
        int divisor = POW5_INT[i];

        // Performs the division (126 bits by 31 bits).
        long wh = (x1 >>> 32);
        long qh = wh / divisor;
        long r = wh - qh * divisor;
        long wl = (r << 32) | (x1 & MASK_32);
        long ql = wl / divisor;
        r = wl - ql * divisor;
        x1 = (qh << 32) | ql;

        wh = (r << 31) | (x0 >>> 32);
        qh = wh / divisor;
        r = wh - qh * divisor;
        wl = (r << 32) | (x0 & MASK_32);
        ql = wl / divisor;
        x0 = (qh << 32) | ql;

        // Adjusts powers.
        n += i;
        pow2 -= i;
      }
      return toDoublePow2(x1, pow2);
    }
  }

  /**
   * Returns the closest <code>double</code> representation of the specified <code>long</code>
   * number
   * multiplied by a power of two.
   *
   * @param m the <code>long</code> multiplier.
   * @param n the power of two exponent.
   * @return <code>m * 2<sup>n</sup></code>.
   */
  private static double toDoublePow2(long m, int n) {
    if (m == 0)
      return 0.0;
    if (m == Long.MIN_VALUE)
      return toDoublePow2(Long.MIN_VALUE >> 1, n + 1);
    if (m < 0)
      return -toDoublePow2(-m, n);
    int bitLength = bitLength(m);
    int shift = bitLength - 53;
    long exp = 1023L + 52 + n + shift; // Use long to avoid overflow.
    if (exp >= 0x7FF)
      return Double.POSITIVE_INFINITY;
    if (exp <= 0) { // Degenerated number (subnormal, assume 0 for bit 52)
      if (exp <= -54)
        return 0.0;
      return toDoublePow2(m, n + 54) / 18014398509481984L; // 2^54 Exact.
    }
    // Normal number.
    long bits = (shift > 0) ? (m >> shift) + ((m >> (shift - 1)) & 1) : // Rounding.
        m << -shift;
    if (((bits >> 52) != 1) && (++exp >= 0x7FF))
      return Double.POSITIVE_INFINITY;
    bits &= 0x000fffffffffffffL; // Clears MSB (bit 52)
    bits |= exp << 52;
    return Double.longBitsToDouble(bits);
  }

  /**
   * Returns the number of bits in the minimal two's-complement representation of the specified
   * <code>long</code>,
   * excluding a sign bit. For positive <code>long</code>, this is equivalent to the number of bits
   * in the ordinary binary representation. For negative <code>long</code>, it is equivalent to the
   * number of bits
   * of the positive value <code>-(l + 1)</code>.
   *
   * @param l the <code>long</code> value for which the bit length is returned.
   * @return the bit length of <code>l</code>.
   */
  private static int bitLength(long l) {
    if (l < 0)
      l = -(l + 1);
    return 64 - numberOfLeadingZeros(l);
  }

  /**
   * Returns the number of zero bits preceding the highest-order ("leftmost") one-bit in the two's
   * complement binary
   * representation of the specified 64 bits unsigned value. Returns 64 if the specified value is
   * zero.
   *
   * @param unsigned the unsigned 64 bits value.
   * @return the number of leading zero bits.
   */
  private static int numberOfLeadingZeros(long unsigned) { // From Hacker's Delight
    if (unsigned == 0) {
      return 64;
    }
    int n = 1;
    int x = (int) (unsigned >>> 32);
    if (x == 0) {
      n += 32;
      x = (int) unsigned;
    }
    if (x >>> 16 == 0) {
      n += 16;
      x <<= 16;
    }
    if (x >>> 24 == 0) {
      n += 8;
      x <<= 8;
    }
    if (x >>> 28 == 0) {
      n += 4;
      x <<= 4;
    }
    if (x >>> 30 == 0) {
      n += 2;
      x <<= 2;
    }
    n -= x >>> 31;
    return n;
  }


  private static final long MASK_63 = 0x7FFFFFFFFFFFFFFFL;

  private static final long MASK_32 = 0xFFFFFFFFL;

  private static final int[] POW5_INT = {1, 5, 25, 125, 625, 3125, 15625,
      78125, 390625, 1953125, 9765625, 48828125, 244140625, 1220703125};



  private static final double LOG2_DIV_LOG10 = 0.3010299956639811952137388947;


  static final int SIGNIFICAND_WIDTH = 53;
  static final int EXP_BIAS = 1023;
  static final long SIGN_BIT_MASK = 0x8000000000000000L;
  static final long SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;
  static final long EXP_BIT_MASK = 0x7FF0000000000000L;


  static final int EXP_SHIFT = SIGNIFICAND_WIDTH - 1;
  static final long FRACT_HOB = (1L << EXP_SHIFT); // assumed High-Order bit
  static final long EXP_ONE = ((long) EXP_BIAS) << EXP_SHIFT; // exponent of 1.0
  static final int MAX_SMALL_BIN_EXP = 62;
  static final int MIN_SMALL_BIN_EXP = -(63 / 3);

  /**
   * @param d The double precision value to convert.
   */
  private static byte[] OPENJDKconvertDoubleToAsciiBytes(double d) {
    long dBits = Double.doubleToRawLongBits(d);
    boolean isNegative = (dBits & SIGN_BIT_MASK) != 0; // discover sign
    long fractBits = dBits & SIGNIF_BIT_MASK;
    int binExp = (int) ((dBits & EXP_BIT_MASK) >> EXP_SHIFT);
    // Discover obvious special cases of NaN and Infinity.
    if (binExp == (int) (EXP_BIT_MASK >> EXP_SHIFT)) {
      if (fractBits == 0L) {
        return isNegative ? bN_INF : bINF;
      } else {
        return bNaN;
      }
    }
    // Finish unpacking
    // Normalize denormalized numbers.
    // Insert assumed high-order bit for normalized numbers.
    // Subtract exponent bias.
    int nSignificantBits;
    if (binExp == 0) {
      if (fractBits == 0L) {
        // not a denorm, just a 0!
        return isNegative ? bNEGATIVE_ZERO : bPOSITIVE_ZERO;
      }
      int leadingZeros = Long.numberOfLeadingZeros(fractBits);
      int shift = leadingZeros - (63 - EXP_SHIFT);
      fractBits <<= shift;
      binExp = 1 - shift;
      nSignificantBits = 64 - leadingZeros; // recall binExp is - shift count.
    } else {
      fractBits |= FRACT_HOB;
      nSignificantBits = EXP_SHIFT + 1;
    }
    binExp -= EXP_BIAS;
    BinaryToASCIIBuffer buf = getBinaryToASCIIBuffer();
    buf.setSign(isNegative);
    // call the routine that actually does all the hard work.
    buf.dtoa(binExp, fractBits, nSignificantBits);
    return buf.getBytes();
  }

  static class BinaryToASCIIBuffer {
    private boolean isNegative;
    private int decExponent;
    private int firstDigitIndex;
    private int nDigits;
    private final byte[] digits = new byte[20];
    private final byte[] buffer = new byte[26];

    /**
     * Default constructor; used for non-zero values,
     * <code>BinaryToASCIIBuffer</code> may be thread-local and reused
     */
    BinaryToASCIIBuffer() {}

    private void setSign(boolean isNegative) {
      this.isNegative = isNegative;
    }

    /**
     * This is the easy subcase --
     * all the significant bits, after scaling, are held in lvalue.
     * negSign and decExponent tell us what processing and scaling
     * has already been done. Exceptional cases have already been
     * stripped out.
     * In particular:
     * lvalue is a finite number (not Inf, nor NaN)
     * lvalue > 0L (not zero, nor negative).
     *
     * The only reason that we develop the digits here, rather than
     * calling on Long.toString() is that we can do it a little faster,
     * and besides want to treat trailing 0s specially. If Long.toString
     * changes, we should re-evaluate this strategy!
     */
    private void developLongDigits(long lvalue, int insignificantDigits) {
      int decExponent = 0;
      if (insignificantDigits != 0) {
        // Discard non-significant low-order bits, while rounding,
        // up to insignificant value.
        long pow10 = FDBigInteger.LONG_5_POW[insignificantDigits] << insignificantDigits; // 10^i ==
                                                                                          // 5^i *
                                                                                          // 2^i;
        long residue = lvalue % pow10;
        lvalue /= pow10;
        decExponent += insignificantDigits;
        if (residue >= (pow10 >> 1)) {
          // round up based on the low-order bits we're discarding
          lvalue++;
        }
      }
      int digitno = digits.length - 1;
      int digit;
      if (lvalue <= Integer.MAX_VALUE) {
        // even easier subcase!
        // can do int arithmetic rather than long!
        int ivalue = (int) lvalue;
        digit = ivalue % 10;
        ivalue /= 10;
        while (digit == 0) {
          decExponent++;
          digit = ivalue % 10;
          ivalue /= 10;
        }
        while (ivalue != 0) {
          digits[digitno--] = digitToAscii(digit);
          decExponent++;
          digit = ivalue % 10;
          ivalue /= 10;
        }
      } else {
        // same algorithm as above (same bugs, too )
        // but using long arithmetic.
        digit = (int) (lvalue % 10L);
        lvalue /= 10L;
        while (digit == 0) {
          decExponent++;
          digit = (int) (lvalue % 10L);
          lvalue /= 10L;
        }
        while (lvalue != 0L) {
          digits[digitno--] = digitToAscii(digit);
          decExponent++;
          digit = (int) (lvalue % 10L);
          lvalue /= 10;
        }
      }
      digits[digitno] = digitToAscii(digit);
      this.decExponent = decExponent + 1;
      this.firstDigitIndex = digitno;
      this.nDigits = this.digits.length - digitno;
    }

    private void dtoa(int binExp, long fractBits, int nSignificantBits) {
      // Examine number. Determine if it is an easy case,
      // which we can do pretty trivially using float/long conversion,
      // or whether we must do real work.
      final int tailZeros = Long.numberOfTrailingZeros(fractBits);

      // number of significant bits of fractBits;
      final int nFractBits = EXP_SHIFT + 1 - tailZeros;

      // number of significant bits to the right of the point.
      int nTinyBits = Math.max(0, nFractBits - binExp - 1);
      if (binExp <= MAX_SMALL_BIN_EXP && binExp >= MIN_SMALL_BIN_EXP) {
        // Look more closely at the number to decide if,
        // with scaling by 10^nTinyBits, the result will fit in
        // a long.
        if ((nTinyBits < FDBigInteger.LONG_5_POW.length)
            && ((nFractBits + N_5_BITS[nTinyBits]) < 64)) {
          //
          // We can do this:
          // take the fraction bits, which are normalized.
          // (a) nTinyBits == 0: Shift left or right appropriately
          // to align the binary point at the extreme right, i.e.
          // where a long int point is expected to be. The integer
          // result is easily converted to a string.
          // (b) nTinyBits > 0: Shift right by EXP_SHIFT-nFractBits,
          // which effectively converts to long and scales by
          // 2^nTinyBits. Then multiply by 5^nTinyBits to
          // complete the scaling. We know this won't overflow
          // because we just counted the number of bits necessary
          // in the result. The integer you get from this can
          // then be converted to a string pretty easily.
          //
          if (nTinyBits == 0) {
            int insignificant;
            if (binExp > nSignificantBits) {
              insignificant = insignificantDigitsForPow2(binExp - nSignificantBits - 1);
            } else {
              insignificant = 0;
            }
            if (binExp >= EXP_SHIFT) {
              fractBits <<= (binExp - EXP_SHIFT);
            } else {
              fractBits >>>= (EXP_SHIFT - binExp);
            }
            developLongDigits(fractBits, insignificant);
            return;
          }
        }
      }

      int decExp = estimateDecExp(fractBits, binExp);
      int B2, B5; // powers of 2 and powers of 5, respectively, in B
      int S2, S5; // powers of 2 and powers of 5, respectively, in S
      int M2, M5; // powers of 2 and powers of 5, respectively, in M

      B5 = Math.max(0, -decExp);
      B2 = B5 + nTinyBits + binExp;

      S5 = Math.max(0, decExp);
      S2 = S5 + nTinyBits;

      M5 = B5;
      M2 = B2 - nSignificantBits;

      //
      // the long integer fractBits contains the (nFractBits) interesting
      // bits from the mantissa of d ( hidden 1 added if necessary) followed
      // by (EXP_SHIFT+1-nFractBits) zeros. In the interest of compactness,
      // I will shift out those zeros before turning fractBits into a
      // FDBigInteger. The resulting whole number will be
      // d * 2^(nFractBits-1-binExp).
      //
      fractBits >>>= tailZeros;
      B2 -= nFractBits - 1;
      int common2factor = Math.min(B2, S2);
      B2 -= common2factor;
      S2 -= common2factor;
      M2 -= common2factor;

      //
      // HACK!! For exact powers of two, the next smallest number
      // is only half as far away as we think (because the meaning of
      // ULP changes at power-of-two bounds) for this reason, we
      // hack M2. Hope this works.
      //
      if (nFractBits == 1) {
        M2 -= 1;
      }

      if (M2 < 0) {
        // oops.
        // since we cannot scale M down far enough,
        // we must scale the other values up.
        B2 -= M2;
        S2 -= M2;
        M2 = 0;
      }
      //
      // Construct, Scale, iterate.
      // Some day, we'll write a stopping test that takes
      // account of the asymmetry of the spacing of floating-point
      // numbers below perfect powers of 2
      // 26 Sept 96 is not that day.
      // So we use a symmetric test.
      //
      int ndigit;
      boolean low, high;
      long lowDigitDifference;
      int q;

      //
      // Detect the special cases where all the numbers we are about
      // to compute will fit in int or long integers.
      // In these cases, we will avoid doing FDBigInteger arithmetic.
      // We use the same algorithms, except that we "normalize"
      // our FDBigIntegers before iterating. This is to make division easier,
      // as it makes our fist guess (quotient of high-order words)
      // more accurate!
      //
      // Some day, we'll write a stopping test that takes
      // account of the asymmetry of the spacing of floating-point
      // numbers below perfect powers of 2
      // 26 Sept 96 is not that day.
      // So we use a symmetric test.
      //
      // binary digits needed to represent B, approx.
      int Bbits = nFractBits + B2 + ((B5 < N_5_BITS.length) ? N_5_BITS[B5] : (B5 * 3));

      // binary digits needed to represent 10*S, approx.
      int tenSbits = S2 + 1 + (((S5 + 1) < N_5_BITS.length) ? N_5_BITS[(S5 + 1)] : ((S5 + 1) * 3));
      if (Bbits < 64 && tenSbits < 64) {
        if (Bbits < 32 && tenSbits < 32) {
          // wa-hoo! They're all ints!
          int b = ((int) fractBits * FDBigInteger.SMALL_5_POW[B5]) << B2;
          int s = FDBigInteger.SMALL_5_POW[S5] << S2;
          int m = FDBigInteger.SMALL_5_POW[M5] << M2;
          int tens = s * 10;
          //
          // Unroll the first iteration. If our decExp estimate
          // was too high, our first quotient will be zero. In this
          // case, we discard it and decrement decExp.
          //
          ndigit = 0;
          q = b / s;
          b = 10 * (b % s);
          m *= 10;
          low = (b < m);
          high = (b + m > tens);
          if ((q == 0) && !high) {
            // oops. Usually ignore leading zero.
            decExp--;
          } else {
            digits[ndigit++] = digitToAscii(q);
          }
          //
          // HACK! Java spec sez that we always have at least
          // one digit after the . in either F- or E-form output.
          // Thus we will need more than one digit if we're using
          // E-form
          //
          if (decExp < -3 || decExp >= 8) {
            high = low = false;
          }
          while (!low && !high) {
            q = b / s;
            b = 10 * (b % s);
            m *= 10;
            if (m > 0L) {
              low = (b < m);
              high = (b + m > tens);
            } else {
              // hack -- m might overflow!
              // in this case, it is certainly > b,
              // which won't
              // and b+m > tens, too, since that has overflowed
              // either!
              low = true;
              high = true;
            }
            digits[ndigit++] = digitToAscii(q);
          }
          lowDigitDifference = ((long) b << 1) - tens;
        } else {
          // still good! they're all longs!
          long b = (fractBits * FDBigInteger.LONG_5_POW[B5]) << B2;
          long s = FDBigInteger.LONG_5_POW[S5] << S2;
          long m = FDBigInteger.LONG_5_POW[M5] << M2;
          long tens = s * 10L;
          //
          // Unroll the first iteration. If our decExp estimate
          // was too high, our first quotient will be zero. In this
          // case, we discard it and decrement decExp.
          //
          ndigit = 0;
          q = (int) (b / s);
          b = 10L * (b % s);
          m *= 10L;
          low = (b < m);
          high = (b + m > tens);
          if ((q == 0) && !high) {
            // oops. Usually ignore leading zero.
            decExp--;
          } else {
            digits[ndigit++] = digitToAscii(q);
          }
          //
          // HACK! Java spec sez that we always have at least
          // one digit after the . in either F- or E-form output.
          // Thus we will need more than one digit if we're using
          // E-form
          //
          if (decExp < -3 || decExp >= 8) {
            high = low = false;
          }
          while (!low && !high) {
            q = (int) (b / s);
            b = 10 * (b % s);
            m *= 10;
            if (m > 0L) {
              low = (b < m);
              high = (b + m > tens);
            } else {
              // hack -- m might overflow!
              // in this case, it is certainly > b,
              // which won't
              // and b+m > tens, too, since that has overflowed
              // either!
              low = true;
              high = true;
            }
            digits[ndigit++] = digitToAscii(q);
          }
          lowDigitDifference = (b << 1) - tens;
        }
      } else {
        //
        // We really must do FDBigInteger arithmetic.
        // Fist, construct our FDBigInteger initial values.
        //
        FDBigInteger Sval = FDBigInteger.valueOfPow52(S5, S2);
        int shiftBias = Sval.getNormalizationBias();
        Sval = Sval.leftShift(shiftBias); // normalize so that division works better

        FDBigInteger Bval = FDBigInteger.valueOfMulPow52(fractBits, B5, B2 + shiftBias);
        FDBigInteger Mval = FDBigInteger.valueOfPow52(M5 + 1, M2 + shiftBias + 1);

        FDBigInteger tenSval = FDBigInteger.valueOfPow52(S5 + 1, S2 + shiftBias + 1); // Sval.mult(
                                                                                      // 10 );
        //
        // Unroll the first iteration. If our decExp estimate
        // was too high, our first quotient will be zero. In this
        // case, we discard it and decrement decExp.
        //
        ndigit = 0;
        q = Bval.quoRemIteration(Sval);
        low = (Bval.cmp(Mval) < 0);
        high = tenSval.addAndCmp(Bval, Mval) <= 0;

        if ((q == 0) && !high) {
          // oops. Usually ignore leading zero.
          decExp--;
        } else {
          digits[ndigit++] = digitToAscii(q);
        }
        //
        // HACK! Java spec sez that we always have at least
        // one digit after the . in either F- or E-form output.
        // Thus we will need more than one digit if we're using
        // E-form
        //
        if (decExp < -3 || decExp >= 8) {
          high = low = false;
        }
        while (!low && !high) {
          q = Bval.quoRemIteration(Sval);
          Mval = Mval.multBy10(); // Mval = Mval.mult( 10 );
          low = (Bval.cmp(Mval) < 0);
          high = tenSval.addAndCmp(Bval, Mval) <= 0;
          digits[ndigit++] = digitToAscii(q);
        }
        if (high && low) {
          Bval = Bval.leftShift(1);
          lowDigitDifference = Bval.cmp(tenSval);
        } else {
          lowDigitDifference = 0L; // this here only for flow analysis!
        }
      }
      this.decExponent = decExp + 1;
      this.firstDigitIndex = 0;
      this.nDigits = ndigit;
      //
      // Last digit gets rounded based on stopping condition.
      //
      if (high) {
        if (low) {
          if (lowDigitDifference == 0L) {
            // it's a tie!
            // choose based on which digits we like.
            if ((digits[firstDigitIndex + nDigits - 1] & 1) != 0) {
              roundup();
            }
          } else if (lowDigitDifference > 0) {
            roundup();
          }
        } else {
          roundup();
        }
      }
    }

    // add one to the least significant digit.
    // in the unlikely event there is a carry out, deal with it.
    // assert that this will only happen where there
    // is only one digit, e.g. (float)1e-44 seems to do it.
    //
    private void roundup() {
      int i = (firstDigitIndex + nDigits - 1);
      int q = digits[i];
      if (q == digitToAscii(9)) {
        while (q == digitToAscii(9) && i > firstDigitIndex) {
          digits[i] = digitToAscii(0);
          q = digits[--i];
        }
        if (q == digitToAscii(9)) {
          // carryout! High-order 1, rest 0s, larger exp.
          decExponent += 1;
          digits[firstDigitIndex] = digitToAscii(1);
          return;
        }
        // else fall through.
      }
      digits[i] = (byte) (q + 1);
    }

    /**
     * Estimate decimal exponent. (If it is small-ish,
     * we could double-check.)
     *
     * First, scale the mantissa bits such that 1 <= d2 < 2.
     * We are then going to estimate
     * log10(d2) ~=~ (d2-1.5)/1.5 + log(1.5)
     * and so we can estimate
     * log10(d) ~=~ log10(d2) + binExp * log10(2)
     * take the floor and call it decExp.
     */
    static int estimateDecExp(long fractBits, int binExp) {
      double d2 = Double.longBitsToDouble(EXP_ONE | (fractBits & SIGNIF_BIT_MASK));
      double d = (d2 - 1.5D) * 0.289529654D + 0.176091259 + (double) binExp * 0.301029995663981;
      long dBits = Double.doubleToRawLongBits(d); // can't be NaN here so use raw
      int exponent = (int) ((dBits & EXP_BIT_MASK) >> EXP_SHIFT) - EXP_BIAS;
      boolean isNegative = (dBits & SIGN_BIT_MASK) != 0; // discover sign
      if (exponent >= 0 && exponent < 52) { // hot path
        long mask = SIGNIF_BIT_MASK >> exponent;
        int r = (int) (((dBits & SIGNIF_BIT_MASK) | FRACT_HOB) >> (EXP_SHIFT - exponent));
        return isNegative ? (((mask & dBits) == 0L) ? -r : -r - 1) : r;
      } else if (exponent < 0) {
        return (((dBits & ~SIGN_BIT_MASK) == 0) ? 0 : ((isNegative) ? -1 : 0));
      } else { // if (exponent >= 52)
        return (int) d;
      }
    }

    /**
     * Calculates
     *
     * <pre>
     * insignificantDigitsForPow2(v) == insignificantDigits(1L << v)
     * </pre>
     */
    private static int insignificantDigitsForPow2(int p2) {
      if (p2 > 1 && p2 < insignificantDigitsNumber.length) {
        return insignificantDigitsNumber[p2];
      }
      return 0;
    }

    /**
     * If insignificant==(1L << ixd)
     * i = insignificantDigitsNumber[idx] is the same as:
     * int i;
     * for ( i = 0; insignificant >= 10L; i++ )
     * insignificant /= 10L;
     */
    private static final int[] insignificantDigitsNumber = {
        0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3,
        4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7,
        8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 11, 11, 11,
        12, 12, 12, 12, 13, 13, 13, 14, 14, 14,
        15, 15, 15, 15, 16, 16, 16, 17, 17, 17,
        18, 18, 18, 19
    };

    // approximately ceil( log2( long5pow[i] ) )
    private static final int[] N_5_BITS = {
        0,
        3,
        5,
        7,
        10,
        12,
        14,
        17,
        19,
        21,
        24,
        26,
        28,
        31,
        33,
        35,
        38,
        40,
        42,
        45,
        47,
        49,
        52,
        54,
        56,
        59,
        61,
    };

    private byte[] getBytes() {
      int i = 0;
      if (isNegative) {
        buffer[0] = bMINUS;
        i = 1;
      }
      if (decExponent > 0 && decExponent < 8) {
        // print digits.digits.
        int byteLength = Math.min(nDigits, decExponent);
        System.arraycopy(digits, firstDigitIndex, buffer, i, byteLength);
        i += byteLength;
        if (byteLength < decExponent) {
          byteLength = decExponent - byteLength;
          Arrays.fill(buffer, i, i + byteLength, digitToAscii(0));
          i += byteLength;
          buffer[i++] = bPERIOD;
          buffer[i++] = digitToAscii(0);
        } else {
          buffer[i++] = bPERIOD;
          if (byteLength < nDigits) {
            int t = nDigits - byteLength;
            System.arraycopy(digits, firstDigitIndex + byteLength, buffer, i, t);
            i += t;
          } else {
            buffer[i++] = digitToAscii(0);
          }
        }
      } else if (decExponent <= 0 && decExponent > -3) {
        buffer[i++] = digitToAscii(0);
        buffer[i++] = bPERIOD;
        if (decExponent != 0) {
          Arrays.fill(buffer, i, i - decExponent, digitToAscii(0));
          i -= decExponent;
        }
        System.arraycopy(digits, firstDigitIndex, buffer, i, nDigits);
        i += nDigits;
      } else {
        buffer[i++] = digits[firstDigitIndex];
        buffer[i++] = bPERIOD;
        if (nDigits > 1) {
          System.arraycopy(digits, firstDigitIndex + 1, buffer, i, nDigits - 1);
          i += nDigits - 1;
        } else {
          buffer[i++] = digitToAscii(0);
        }
        buffer[i++] = bUPPERCASE_E;
        int e;
        if (decExponent <= 0) {
          buffer[i++] = bMINUS;
          e = -decExponent + 1;
        } else {
          e = decExponent - 1;
        }
        // decExponent has 1, 2, or 3, digits
        if (e <= 9) {
          buffer[i++] = digitToAscii(e);
        } else if (e <= 99) {
          buffer[i++] = digitToAscii(e / 10);
          buffer[i++] = digitToAscii(e % 10);
        } else {
          buffer[i++] = digitToAscii(e / 100);
          e %= 100;
          buffer[i++] = digitToAscii(e / 10);
          buffer[i++] = digitToAscii(e & 10);
        }
      }
      if (i >= 2) {
        // if it ends with ".0" strip it off
        if (buffer[i - 2] == bPERIOD && buffer[i - 1] == digitToAscii(0)) {
          i -= 2;
        }
      }
      return ByteArrays.copy(buffer, 0, i);
    }
  }

  private static final ThreadLocal<BinaryToASCIIBuffer> threadLocalBinaryToASCIIBuffer =
      ThreadLocal.withInitial(BinaryToASCIIBuffer::new);

  private static BinaryToASCIIBuffer getBinaryToASCIIBuffer() {
    return threadLocalBinaryToASCIIBuffer.get();
  }

  /**
   * A simple big integer package specifically for floating point base conversion.
   */
  private static class FDBigInteger {

    static final int[] SMALL_5_POW;

    static final long[] LONG_5_POW;

    // Maximum size of cache of powers of 5 as FDBigIntegers.
    private static final int MAX_FIVE_POW = 340;

    // Cache of big powers of 5 as FDBigIntegers.
    private static final FDBigInteger[] POW_5_CACHE;

    // Zero as an FDBigInteger.
    public static final FDBigInteger ZERO;

    // Initialize FDBigInteger cache of powers of 5.
    static {
      long[] long5pow = {
          1L,
          5L,
          5L * 5,
          5L * 5 * 5,
          5L * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
              * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
              * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
              * 5 * 5,
          5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
              * 5 * 5 * 5,
      };
      int[] small5pow = {
          1,
          5,
          5 * 5,
          5 * 5 * 5,
          5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
          5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
      };
      FDBigInteger[] pow5cache = new FDBigInteger[MAX_FIVE_POW];
      int i = 0;
      while (i < small5pow.length) {
        FDBigInteger pow5 = new FDBigInteger(new int[] {small5pow[i]}, 0);
        pow5.makeImmutable();
        pow5cache[i] = pow5;
        i++;
      }
      FDBigInteger prev = pow5cache[i - 1];
      while (i < MAX_FIVE_POW) {
        pow5cache[i] = prev = prev.mult(5);
        prev.makeImmutable();
        i++;
      }
      FDBigInteger zero = new FDBigInteger(new int[0], 0);
      zero.makeImmutable();
      SMALL_5_POW = small5pow;
      LONG_5_POW = long5pow;
      POW_5_CACHE = pow5cache;
      ZERO = zero;
    }

    // Constant for casting an int to a long via bitwise AND.
    private static final long LONG_MASK = 0xffffffffL;

    // @ spec_public non_null;
    private int[] data; // value: data[0] is least significant
    // @ spec_public;
    private int offset; // number of least significant zero padding ints
    // @ spec_public;
    private int nWords; // data[nWords-1]!=0, all values above are zero
    // if nWords==0 -> this FDBigInteger is zero
    // @ spec_public;
    private boolean isImmutable = false;

    /**
     * Constructs an <code>FDBigInteger</code> from data and padding. The
     * <code>data</code> parameter has the least significant <code>int</code> at
     * the zeroth index. The <code>offset</code> parameter gives the number of
     * zero <code>int</code>s to be inferred below the least significant element
     * of <code>data</code>.
     *
     * @param data An array containing all non-zero <code>int</code>s of the value.
     * @param offset An offset indicating the number of zero <code>int</code>s to pad
     *        below the least significant element of <code>data</code>.
     */
    private FDBigInteger(int[] data, int offset) {
      this.data = data;
      this.offset = offset;
      this.nWords = data.length;
      trimLeadingZeros();
    }

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>5<sup>p5</sup> * 2<sup>p2</sup></code>.
     *
     * @param p5 The exponent of the power-of-five factor.
     * @param p2 The exponent of the power-of-two factor.
     * @return <code>5<sup>p5</sup> * 2<sup>p2</sup></code>
     */
    public static FDBigInteger valueOfPow52(int p5, int p2) {
      if (p5 != 0) {
        if (p2 == 0) {
          return big5pow(p5);
        } else if (p5 < SMALL_5_POW.length) {
          int pow5 = SMALL_5_POW[p5];
          int wordcount = p2 >> 5;
          int bitcount = p2 & 0x1f;
          if (bitcount == 0) {
            return new FDBigInteger(new int[] {pow5}, wordcount);
          } else {
            return new FDBigInteger(new int[] {
                pow5 << bitcount,
                pow5 >>> (32 - bitcount)
            }, wordcount);
          }
        } else {
          return big5pow(p5).leftShift(p2);
        }
      } else {
        return valueOfPow2(p2);
      }
    }

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>value * 5<sup>p5</sup> * 2<sup>p2</sup></code>.
     *
     * @param value The constant factor.
     * @param p5 The exponent of the power-of-five factor.
     * @param p2 The exponent of the power-of-two factor.
     * @return <code>value * 5<sup>p5</sup> * 2<sup>p2</sup></code>
     */
    public static FDBigInteger valueOfMulPow52(long value, int p5, int p2) {
      int v0 = (int) value;
      int v1 = (int) (value >>> 32);
      int wordcount = p2 >> 5;
      int bitcount = p2 & 0x1f;
      if (p5 != 0) {
        if (p5 < SMALL_5_POW.length) {
          long pow5 = SMALL_5_POW[p5] & LONG_MASK;
          long carry = (v0 & LONG_MASK) * pow5;
          v0 = (int) carry;
          carry >>>= 32;
          carry = (v1 & LONG_MASK) * pow5 + carry;
          v1 = (int) carry;
          int v2 = (int) (carry >>> 32);
          if (bitcount == 0) {
            return new FDBigInteger(new int[] {v0, v1, v2}, wordcount);
          } else {
            return new FDBigInteger(new int[] {
                v0 << bitcount,
                (v1 << bitcount) | (v0 >>> (32 - bitcount)),
                (v2 << bitcount) | (v1 >>> (32 - bitcount)),
                v2 >>> (32 - bitcount)
            }, wordcount);
          }
        } else {
          FDBigInteger pow5 = big5pow(p5);
          int[] r;
          if (v1 == 0) {
            r = new int[pow5.nWords + 1 + ((p2 != 0) ? 1 : 0)];
            mult(pow5.data, pow5.nWords, v0, r);
          } else {
            r = new int[pow5.nWords + 2 + ((p2 != 0) ? 1 : 0)];
            mult(pow5.data, pow5.nWords, v0, v1, r);
          }
          return (new FDBigInteger(r, pow5.offset)).leftShift(p2);
        }
      } else if (p2 != 0) {
        if (bitcount == 0) {
          return new FDBigInteger(new int[] {v0, v1}, wordcount);
        } else {
          return new FDBigInteger(new int[] {
              v0 << bitcount,
              (v1 << bitcount) | (v0 >>> (32 - bitcount)),
              v1 >>> (32 - bitcount)
          }, wordcount);
        }
      }
      return new FDBigInteger(new int[] {v0, v1}, 0);
    }

    /**
     * Returns an <code>FDBigInteger</code> with the numerical value
     * <code>2<sup>p2</sup></code>.
     *
     * @param p2 The exponent of 2.
     * @return <code>2<sup>p2</sup></code>
     */
    private static FDBigInteger valueOfPow2(int p2) {
      int wordcount = p2 >> 5;
      int bitcount = p2 & 0x1f;
      return new FDBigInteger(new int[] {1 << bitcount}, wordcount);
    }

    /**
     * Removes all leading zeros from this <code>FDBigInteger</code> adjusting
     * the offset and number of non-zero leading words accordingly.
     */
    private void trimLeadingZeros() {
      int i = nWords;
      if (i > 0 && (data[--i] == 0)) {
        // for (; i > 0 && data[i - 1] == 0; i--) ;
        while (i > 0 && data[i - 1] == 0) {
          i--;
        }
        this.nWords = i;
        if (i == 0) { // all words are zero
          this.offset = 0;
        }
      }
    }

    /**
     * Retrieves the normalization bias of the <code>FDBigIntger</code>. The
     * normalization bias is a left shift such that after it the highest word
     * of the value will have the 4 highest bits equal to zero:
     * {@code (highestWord & 0xf0000000) == 0}, but the next bit should be 1
     * {@code (highestWord & 0x08000000) != 0}.
     *
     * @return The normalization bias.
     */
    public int getNormalizationBias() {
      if (nWords == 0) {
        throw new IllegalArgumentException("Zero value cannot be normalized");
      }
      int zeros = Integer.numberOfLeadingZeros(data[nWords - 1]);
      return (zeros < 4) ? 28 + zeros : zeros - 4;
    }

    // TODO: Why is anticount param needed if it is always 32 - bitcount?
    /**
     * Left shifts the contents of one int array into another.
     *
     * @param src The source array.
     * @param idx The initial index of the source array.
     * @param result The destination array.
     * @param bitcount The left shift.
     * @param anticount The left anti-shift, e.g., <code>32-bitcount</code>.
     * @param prev The prior source value.
     */
    private static void leftShift(int[] src, int idx, int[] result, int bitcount, int anticount,
        int prev) {
      for (; idx > 0; idx--) {
        int v = (prev << bitcount);
        prev = src[idx - 1];
        v |= (prev >>> anticount);
        result[idx] = v;
      }
      int v = prev << bitcount;
      result[0] = v;
    }

    /**
     * Shifts this <code>FDBigInteger</code> to the left. The shift is performed
     * in-place unless the <code>FDBigInteger</code> is immutable in which case
     * a new instance of <code>FDBigInteger</code> is returned.
     *
     * @param shift The number of bits to shift left.
     * @return The shifted <code>FDBigInteger</code>.
     */
    public FDBigInteger leftShift(int shift) {
      if (shift == 0 || nWords == 0) {
        return this;
      }
      int wordcount = shift >> 5;
      int bitcount = shift & 0x1f;
      if (this.isImmutable) {
        if (bitcount == 0) {
          return new FDBigInteger(Arrays.copyOf(data, nWords), offset + wordcount);
        } else {
          int anticount = 32 - bitcount;
          int idx = nWords - 1;
          int prev = data[idx];
          int hi = prev >>> anticount;
          int[] result;
          if (hi != 0) {
            result = new int[nWords + 1];
            result[nWords] = hi;
          } else {
            result = new int[nWords];
          }
          leftShift(data, idx, result, bitcount, anticount, prev);
          return new FDBigInteger(result, offset + wordcount);
        }
      } else {
        if (bitcount != 0) {
          int anticount = 32 - bitcount;
          if ((data[0] << bitcount) == 0) {
            int idx = 0;
            int prev = data[idx];
            for (; idx < nWords - 1; idx++) {
              int v = (prev >>> anticount);
              prev = data[idx + 1];
              v |= (prev << bitcount);
              data[idx] = v;
            }
            int v = prev >>> anticount;
            data[idx] = v;
            if (v == 0) {
              nWords--;
            }
            offset++;
          } else {
            int idx = nWords - 1;
            int prev = data[idx];
            int hi = prev >>> anticount;
            int[] result = data;
            int[] src = data;
            if (hi != 0) {
              if (nWords == data.length) {
                data = result = new int[nWords + 1];
              }
              result[nWords++] = hi;
            }
            leftShift(src, idx, result, bitcount, anticount, prev);
          }
        }
        offset += wordcount;
        return this;
      }
    }

    /**
     * Returns the number of <code>int</code>s this <code>FDBigInteger</code> represents.
     *
     * @return Number of <code>int</code>s required to represent this <code>FDBigInteger</code>.
     */
    private int size() {
      return nWords + offset;
    }


    /**
     * Computes
     *
     * <pre>
     * q = (int)( this / S )
     * this = 10 * ( this mod S )
     * Return q.
     * </pre>
     *
     * This is the iteration step of digit development for output.
     * We assume that S has been normalized, as above, and that
     * "this" has been left-shifted accordingly.
     * Also assumed, of course, is that the result, q, can be expressed
     * as an integer, {@code 0 <= q < 10}.
     *
     * @param S The divisor of this <code>FDBigInteger</code>.
     * @return <code>q = (int)(this / S)</code>.
     */
    public int quoRemIteration(FDBigInteger S) throws IllegalArgumentException {
      // ensure that this and S have the same number of
      // digits. If S is properly normalized and q < 10 then
      // this must be so.
      int thSize = this.size();
      int sSize = S.size();
      if (thSize < sSize) {
        // this value is significantly less than S, result of division is zero.
        // just mult this by 10.
        int p = multAndCarryBy10(this.data, this.nWords, this.data);
        if (p != 0) {
          this.data[nWords++] = p;
        } else {
          trimLeadingZeros();
        }
        return 0;
      } else if (thSize > sSize) {
        throw new IllegalArgumentException("disparate values");
      }
      // estimate q the obvious way. We will usually be
      // right. If not, then we're only off by a little and
      // will re-add.
      long q = (this.data[this.nWords - 1] & LONG_MASK) / (S.data[S.nWords - 1] & LONG_MASK);
      long diff = multDiffMe(q, S);
      if (diff != 0L) {
        // q is too big.
        // add S back in until this turns +. This should
        // not be very many times!
        long sum = 0L;
        int tStart = S.offset - this.offset;
        int[] sd = S.data;
        int[] td = this.data;
        while (sum == 0L) {
          for (int sIndex = 0, tIndex = tStart; tIndex < this.nWords; sIndex++, tIndex++) {
            sum += (td[tIndex] & LONG_MASK) + (sd[sIndex] & LONG_MASK);
            td[tIndex] = (int) sum;
            sum >>>= 32; // Signed or unsigned, answer is 0 or 1
          }
          q -= 1;
        }
      }
      // finally, we can multiply this by 10.
      // it cannot overflow, right, as the high-order word has
      // at least 4 high-order zeros!
      multAndCarryBy10(this.data, this.nWords, this.data);
      trimLeadingZeros();
      return (int) q;
    }

    /**
     * Multiplies this <code>FDBigInteger</code> by 10. The operation will be
     * performed in place unless the <code>FDBigInteger</code> is immutable in
     * which case a new <code>FDBigInteger</code> will be returned.
     *
     * @return The <code>FDBigInteger</code> multiplied by 10.
     */
    public FDBigInteger multBy10() {
      if (nWords == 0) {
        return this;
      }
      if (isImmutable) {
        int[] res = new int[nWords + 1];
        res[nWords] = multAndCarryBy10(data, nWords, res);
        return new FDBigInteger(res, offset);
      } else {
        int p = multAndCarryBy10(this.data, this.nWords, this.data);
        if (p != 0) {
          if (nWords == data.length) {
            if (data[0] == 0) {
              System.arraycopy(data, 1, data, 0, --nWords);
              offset++;
            } else {
              data = Arrays.copyOf(data, data.length + 1);
            }
          }
          data[nWords++] = p;
        } else {
          trimLeadingZeros();
        }
        return this;
      }
    }

    /**
     * Multiplies two big integers represented as int arrays.
     *
     * @param s1 The first array factor.
     * @param s1Len The number of elements of <code>s1</code> to use.
     * @param s2 The second array factor.
     * @param s2Len The number of elements of <code>s2</code> to use.
     * @param dst The product array.
     */
    private static void mult(int[] s1, int s1Len, int[] s2, int s2Len, int[] dst) {
      for (int i = 0; i < s1Len; i++) {
        long v = s1[i] & LONG_MASK;
        long p = 0L;
        for (int j = 0; j < s2Len; j++) {
          p += (dst[i + j] & LONG_MASK) + v * (s2[j] & LONG_MASK);
          dst[i + j] = (int) p;
          p >>>= 32;
        }
        dst[i + s2Len] = (int) p;
      }
    }

    /**
     * Determines whether all elements of an array are zero for all indices less
     * than a given index.
     *
     * @param a The array to be examined.
     * @param from The index strictly below which elements are to be examined.
     * @return Zero if all elements in range are zero, 1 otherwise.
     */
    private static int checkZeroTail(int[] a, int from) {
      while (from > 0) {
        if (a[--from] != 0) {
          return 1;
        }
      }
      return 0;
    }

    /**
     * Compares the parameter with this <code>FDBigInteger</code>. Returns an
     * integer accordingly as:
     *
     * <pre>
     * {@code
     * > 0: this > other
     *   0: this == other
     * < 0: this < other
     * }
     * </pre>
     *
     * @param other The <code>FDBigInteger</code> to compare.
     * @return A negative value, zero, or a positive value according to the
     *         result of the comparison.
     */
    public int cmp(FDBigInteger other) {
      int aSize = nWords + offset;
      int bSize = other.nWords + other.offset;
      if (aSize > bSize) {
        return 1;
      } else if (aSize < bSize) {
        return -1;
      }
      int aLen = nWords;
      int bLen = other.nWords;
      while (aLen > 0 && bLen > 0) {
        int a = data[--aLen];
        int b = other.data[--bLen];
        if (a != b) {
          return ((a & LONG_MASK) < (b & LONG_MASK)) ? -1 : 1;
        }
      }
      if (aLen > 0) {
        return checkZeroTail(data, aLen);
      }
      if (bLen > 0) {
        return -checkZeroTail(other.data, bLen);
      }
      return 0;
    }

    /**
     * Compares this <code>FDBigInteger</code> with <code>x + y</code>. Returns a
     * value according to the comparison as:
     *
     * <pre>
     * {@code
     * -1: this <  x + y
     *  0: this == x + y
     *  1: this >  x + y
     * }
     * </pre>
     *
     * @param x The first addend of the sum to compare.
     * @param y The second addend of the sum to compare.
     * @return -1, 0, or 1 according to the result of the comparison.
     */
    public int addAndCmp(FDBigInteger x, FDBigInteger y) {
      FDBigInteger big;
      FDBigInteger small;
      int xSize = x.size();
      int ySize = y.size();
      int bSize;
      int sSize;
      if (xSize >= ySize) {
        big = x;
        small = y;
        bSize = xSize;
        sSize = ySize;
      } else {
        big = y;
        small = x;
        bSize = ySize;
        sSize = xSize;
      }
      int thSize = this.size();
      if (bSize == 0) {
        return thSize == 0 ? 0 : 1;
      }
      if (sSize == 0) {
        return this.cmp(big);
      }
      if (bSize > thSize) {
        return -1;
      }
      if (bSize + 1 < thSize) {
        return 1;
      }
      long top = (big.data[big.nWords - 1] & LONG_MASK);
      if (sSize == bSize) {
        top += (small.data[small.nWords - 1] & LONG_MASK);
      }
      if ((top >>> 32) == 0) {
        if (((top + 1) >>> 32) == 0) {
          // good case - no carry extension
          if (bSize < thSize) {
            return 1;
          }
          // here sum.nWords == this.nWords
          long v = (this.data[this.nWords - 1] & LONG_MASK);
          if (v < top) {
            return -1;
          }
          if (v > top + 1) {
            return 1;
          }
        }
      } else { // (top>>>32)!=0 guaranteed carry extension
        if (bSize + 1 > thSize) {
          return -1;
        }
        // here sum.nWords == this.nWords
        top >>>= 32;
        long v = (this.data[this.nWords - 1] & LONG_MASK);
        if (v < top) {
          return -1;
        }
        if (v > top + 1) {
          return 1;
        }
      }
      return this.cmp(big.add(small));
    }

    /**
     * Makes this <code>FDBigInteger</code> immutable.
     */
    public void makeImmutable() {
      this.isImmutable = true;
    }

    /**
     * Multiplies this <code>FDBigInteger</code> by an integer.
     *
     * @param i The factor by which to multiply this <code>FDBigInteger</code>.
     * @return This <code>FDBigInteger</code> multiplied by an integer.
     */
    private FDBigInteger mult(int i) {
      if (this.nWords == 0) {
        return this;
      }
      int[] r = new int[nWords + 1];
      mult(data, nWords, i, r);
      return new FDBigInteger(r, offset);
    }

    /**
     * Multiplies this <code>FDBigInteger</code> by another <code>FDBigInteger</code>.
     *
     * @param other The <code>FDBigInteger</code> factor by which to multiply.
     * @return The product of this and the parameter <code>FDBigInteger</code>s.
     */
    private FDBigInteger mult(FDBigInteger other) {
      if (this.nWords == 0) {
        return this;
      }
      if (this.size() == 1) {
        return other.mult(data[0]);
      }
      if (other.nWords == 0) {
        return other;
      }
      if (other.size() == 1) {
        return this.mult(other.data[0]);
      }
      int[] r = new int[nWords + other.nWords];
      mult(this.data, this.nWords, other.data, other.nWords, r);
      return new FDBigInteger(r, this.offset + other.offset);
    }

    /**
     * Adds another <code>FDBigInteger</code> to this <code>FDBigInteger</code>.
     *
     * @param other The <code>FDBigInteger</code> to add.
     * @return The sum of the <code>FDBigInteger</code>s.
     */
    private FDBigInteger add(FDBigInteger other) {
      FDBigInteger big, small;
      int bigLen, smallLen;
      int tSize = this.size();
      int oSize = other.size();
      if (tSize >= oSize) {
        big = this;
        bigLen = tSize;
        small = other;
        smallLen = oSize;
      } else {
        big = other;
        bigLen = oSize;
        small = this;
        smallLen = tSize;
      }
      int[] r = new int[bigLen + 1];
      int i = 0;
      long carry = 0L;
      for (; i < smallLen; i++) {
        carry += (i < big.offset ? 0L : (big.data[i - big.offset] & LONG_MASK))
            + ((i < small.offset ? 0L : (small.data[i - small.offset] & LONG_MASK)));
        r[i] = (int) carry;
        carry >>= 32; // signed shift.
      }
      for (; i < bigLen; i++) {
        carry += (i < big.offset ? 0L : (big.data[i - big.offset] & LONG_MASK));
        r[i] = (int) carry;
        carry >>= 32; // signed shift.
      }
      r[bigLen] = (int) carry;
      return new FDBigInteger(r, 0);
    }

    /**
     * Multiplies the parameters and subtracts them from this
     * <code>FDBigInteger</code>.
     *
     * @param q The integer parameter.
     * @param S The <code>FDBigInteger</code> parameter.
     * @return <code>this - q*S</code>.
     */
    private long multDiffMe(long q, FDBigInteger S) {
      long diff = 0L;
      if (q != 0) {
        int deltaSize = S.offset - this.offset;
        if (deltaSize >= 0) {
          int[] sd = S.data;
          int[] td = this.data;
          for (int sIndex = 0, tIndex = deltaSize; sIndex < S.nWords; sIndex++, tIndex++) {
            diff += (td[tIndex] & LONG_MASK) - q * (sd[sIndex] & LONG_MASK);
            td[tIndex] = (int) diff;
            diff >>= 32; // N.B. SIGNED shift.
          }
        } else {
          deltaSize = -deltaSize;
          int[] rd = new int[nWords + deltaSize];
          int sIndex = 0;
          int rIndex = 0;
          int[] sd = S.data;
          for (; rIndex < deltaSize && sIndex < S.nWords; sIndex++, rIndex++) {
            diff -= q * (sd[sIndex] & LONG_MASK);
            rd[rIndex] = (int) diff;
            diff >>= 32; // N.B. SIGNED shift.
          }
          int tIndex = 0;
          int[] td = this.data;
          for (; sIndex < S.nWords; sIndex++, tIndex++, rIndex++) {
            diff += (td[tIndex] & LONG_MASK) - q * (sd[sIndex] & LONG_MASK);
            rd[rIndex] = (int) diff;
            diff >>= 32; // N.B. SIGNED shift.
          }
          this.nWords += deltaSize;
          this.offset -= deltaSize;
          this.data = rd;
        }
      }
      return diff;
    }


    /**
     * Multiplies by 10 a big integer represented as an array. The final carry
     * is returned.
     *
     * @param src The array representation of the big integer.
     * @param srcLen The number of elements of <code>src</code> to use.
     * @param dst The product array.
     * @return The final carry of the multiplication.
     */
    private static int multAndCarryBy10(int[] src, int srcLen, int[] dst) {
      long carry = 0;
      for (int i = 0; i < srcLen; i++) {
        long product = (src[i] & LONG_MASK) * 10L + carry;
        dst[i] = (int) product;
        carry = product >>> 32;
      }
      return (int) carry;
    }

    /**
     * Multiplies by a constant value a big integer represented as an array.
     * The constant factor is an <code>int</code>.
     *
     * @param src The array representation of the big integer.
     * @param srcLen The number of elements of <code>src</code> to use.
     * @param value The constant factor by which to multiply.
     * @param dst The product array.
     */
    private static void mult(int[] src, int srcLen, int value, int[] dst) {
      long val = value & LONG_MASK;
      long carry = 0;
      for (int i = 0; i < srcLen; i++) {
        long product = (src[i] & LONG_MASK) * val + carry;
        dst[i] = (int) product;
        carry = product >>> 32;
      }
      dst[srcLen] = (int) carry;
    }

    /**
     * Multiplies by a constant value a big integer represented as an array.
     * The constant factor is a long represent as two <code>int</code>s.
     *
     * @param src The array representation of the big integer.
     * @param srcLen The number of elements of <code>src</code> to use.
     * @param v0 The lower 32 bits of the long factor.
     * @param v1 The upper 32 bits of the long factor.
     * @param dst The product array.
     */
    private static void mult(int[] src, int srcLen, int v0, int v1, int[] dst) {
      long v = v0 & LONG_MASK;
      long carry = 0;
      for (int j = 0; j < srcLen; j++) {
        long product = v * (src[j] & LONG_MASK) + carry;
        dst[j] = (int) product;
        carry = product >>> 32;
      }
      dst[srcLen] = (int) carry;
      v = v1 & LONG_MASK;
      carry = 0;
      for (int j = 0; j < srcLen; j++) {
        long product = (dst[j + 1] & LONG_MASK) + v * (src[j] & LONG_MASK) + carry;
        dst[j + 1] = (int) product;
        carry = product >>> 32;
      }
      dst[srcLen + 1] = (int) carry;
    }

    // Fails assertion for negative exponent.
    /**
     * Computes <code>5</code> raised to a given power.
     *
     * @param p The exponent of 5.
     * @return <code>5<sup>p</sup></code>.
     */
    private static FDBigInteger big5pow(int p) {
      if (p < MAX_FIVE_POW) {
        return POW_5_CACHE[p];
      }
      return big5powRec(p);
    }

    // slow path
    /**
     * Computes <code>5</code> raised to a given power.
     *
     * @param p The exponent of 5.
     * @return <code>5<sup>p</sup></code>.
     */
    private static FDBigInteger big5powRec(int p) {
      if (p < MAX_FIVE_POW) {
        return POW_5_CACHE[p];
      }
      // construct the value.
      // recursively.
      int q, r;
      // in order to compute 5^p,
      // compute its square root, 5^(p/2) and square.
      // or, let q = p / 2, r = p -q, then
      // 5^p = 5^(q+r) = 5^q * 5^r
      q = p >> 1;
      r = p - q;
      FDBigInteger bigq = big5powRec(q);
      if (r < SMALL_5_POW.length) {
        return bigq.mult(SMALL_5_POW[r]);
      } else {
        return bigq.mult(big5powRec(r));
      }
    }

    // for debugging ...
    /**
     * Converts this <code>FDBigInteger</code> to a <code>BigInteger</code>.
     *
     * @return The <code>BigInteger</code> representation.
     */
    public BigInteger toBigInteger() {
      byte[] magnitude = new byte[nWords * 4 + 1];
      for (int i = 0; i < nWords; i++) {
        int w = data[i];
        magnitude[magnitude.length - 4 * i - 1] = (byte) w;
        magnitude[magnitude.length - 4 * i - 2] = (byte) (w >> 8);
        magnitude[magnitude.length - 4 * i - 3] = (byte) (w >> 16);
        magnitude[magnitude.length - 4 * i - 4] = (byte) (w >> 24);
      }
      return new BigInteger(magnitude).shiftLeft(offset * 32);
    }

    // for debugging ...
    /**
     * Converts this <code>FDBigInteger</code> to a string.
     *
     * @return The string representation.
     */
    @Override
    public String toString() {
      return toBigInteger().toString();
    }
  }
}
