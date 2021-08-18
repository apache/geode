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
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_A;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_Z;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMINUS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMOVED;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNIL;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNaN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOK;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOOM;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPERIOD;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWRONGTYPE;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBuf;

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
      buffer.writeByte(INTEGER_ID);
      appendAsciiDigitsToByteBuf((Integer) v, buffer);
      buffer.writeBytes(bCRLF);
    } else if (v instanceof Long) {
      buffer.writeByte(INTEGER_ID);
      appendAsciiDigitsToByteBuf((Long) v, buffer);
      buffer.writeBytes(bCRLF);
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

  public static String doubleToString(double d) {
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

  public static byte[] intToBytes(int i) {
    return stringToBytes(String.valueOf(i));
  }

  public static byte[] longToBytes(long l) {
    return stringToBytes(String.valueOf(l));
  }

  public static byte[] doubleToBytes(double d) {
    return stringToBytes(doubleToString(d));
  }

  public static byte[] bigDecimalToBytes(BigDecimal b) {
    return stringToBytes(b.toPlainString());
  }

  public static BigDecimal bytesToBigDecimal(byte[] bytes) {
    return new BigDecimal(bytesToString(bytes));
  }

  public static long bytesToLong(byte[] bytes) {
    return Long.parseLong(bytesToString(bytes));
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
    if (isPositiveInfinity(bytes)) {
      return POSITIVE_INFINITY;
    }
    if (isNegativeInfinity(bytes)) {
      return NEGATIVE_INFINITY;
    }
    if (isNaN(bytes)) {
      return NaN;
    }
    return stringToDouble(bytesToString(bytes));
  }

  /**
   * Redis specific manner to parse floats
   *
   * @param d String holding double
   * @return Value of string
   * @throws NumberFormatException if the double cannot be parsed
   */
  public static double stringToDouble(String d) {
    if (d.equalsIgnoreCase(P_INF)) {
      return Double.POSITIVE_INFINITY;
    } else if (d.equalsIgnoreCase(N_INF)) {
      return Double.NEGATIVE_INFINITY;
    } else {
      return Double.parseDouble(d);
    }
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
   * This code was adapted from the openjdk Long.java getChars methods.
   */
  public static void appendAsciiDigitsToByteBuf(long value, ByteBuf buf) {
    if (value < 0) {
      buf.writeByte(bMINUS);
    } else {
      value = -value;
    }
    // at this point value <= 0

    if (value > -100) {
      // it has at most two digits: [0..99]
      appendSmallAsciiDigitsToByteBuf((int) value, buf);
    } else {
      appendLargeAsciiDigitsToByteBuf(value, buf);
    }
  }

  private static void appendSmallAsciiDigitsToByteBuf(int value, ByteBuf buf) {
    int q = value / 10;
    int r = (q * 10) - value;
    if (q < 0) {
      buf.writeByte(digitToAscii(-q));
    }
    buf.writeByte(digitToAscii(r));
  }

  private static void appendLargeAsciiDigitsToByteBuf(long value, ByteBuf buf) {
    long q;
    int r;
    final int MAX_DIGITS = 19;
    int charPos = MAX_DIGITS;
    byte[] bytes = new byte[MAX_DIGITS];

    // Get 2 digits/iteration using longs until quotient fits into an int
    while (value <= Integer.MIN_VALUE) {
      q = value / 100;
      r = (int) ((q * 100) - value);
      value = q;
      bytes[--charPos] = DIGIT_ONES[r];
      bytes[--charPos] = DIGIT_TENS[r];
    }

    // Get 2 digits/iteration using ints
    int q2;
    int i2 = (int) value;
    while (i2 <= -100) {
      q2 = i2 / 100;
      r = (q2 * 100) - i2;
      i2 = q2;
      bytes[--charPos] = DIGIT_ONES[r];
      bytes[--charPos] = DIGIT_TENS[r];
    }

    // We know there are at most two digits left at this point.
    q2 = i2 / 10;
    r = (q2 * 10) - i2;
    bytes[--charPos] = digitToAscii(r);

    // Whatever left is the remaining digit.
    if (q2 < 0) {
      bytes[--charPos] = digitToAscii(-q2);
    }
    buf.writeBytes(bytes, charPos, MAX_DIGITS - charPos);
  }

  /**
   * Given a digit (a value in the range 0..9) return its corresponding ASCII code.
   * NOTE: the caller is responsible for ensuring that 'digit' is in the correct range.
   */
  public static byte digitToAscii(int digit) {
    return (byte) (NUMBER_0_BYTE + digit);
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
}
