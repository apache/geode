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
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.N_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.P_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.SIMPLE_STRING_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCRLF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bEMPTY_ARRAY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bEMPTY_STRING;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bERR;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bINFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_A;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLOWERCASE_Z;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMOVED;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNIL;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bN_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNaN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOK;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bOOM;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INF;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bP_INFINITY;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWRONGTYPE;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBuf;

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
      buffer.writeBytes(intToBytes((Integer) v));
      buffer.writeBytes(bCRLF);
    } else if (v instanceof Long) {
      buffer.writeByte(INTEGER_ID);
      buffer.writeBytes(intToBytes(((Long) v).intValue()));
      buffer.writeBytes(bCRLF);
    } else {
      throw new CoderException();
    }

    return buffer;
  }

  private static void writeStringResponse(ByteBuf buffer, byte[] toWrite) {
    buffer.writeByte(BULK_STRING_ID);
    buffer.writeBytes(intToBytes(toWrite.length));
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
    buffer.writeBytes(intToBytes(items.size()));
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
    buffer.writeBytes(intToBytes(2));
    buffer.writeBytes(bCRLF);
    buffer.writeByte(BULK_STRING_ID);
    byte[] cursorBytes = stringToBytes(cursor.toString());
    buffer.writeBytes(intToBytes(cursorBytes.length));
    buffer.writeBytes(bCRLF);
    buffer.writeBytes(cursorBytes);
    buffer.writeBytes(bCRLF);
    buffer.writeByte(ARRAY_ID);
    buffer.writeBytes(intToBytes(scanResult.size()));
    buffer.writeBytes(bCRLF);

    for (Object nextObject : scanResult) {
      byte[] bytes;
      if (nextObject instanceof String) {
        String next = (String) nextObject;
        bytes = stringToBytes(next);
      } else if (nextObject instanceof RedisKey) {
        bytes = ((RedisKey) nextObject).toBytes();
      } else {
        bytes = (byte[]) nextObject;
      }

      buffer.writeByte(BULK_STRING_ID);
      buffer.writeBytes(intToBytes(bytes.length));
      buffer.writeBytes(bCRLF);
      buffer.writeBytes(bytes);
      buffer.writeBytes(bCRLF);
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

  public static ByteBuf getCustomErrorResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, int integer) {
    buffer.writeByte(INTEGER_ID);
    buffer.writeBytes(intToBytes(integer));
    buffer.writeBytes(bCRLF);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, long l) {
    buffer.writeByte(INTEGER_ID);
    buffer.writeBytes(longToBytes(l));
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
      return "Infinity";
    }
    if (d == Double.NEGATIVE_INFINITY) {
      return "-Infinity";
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

  public static int bytesToInt(byte[] bytes) {
    return Integer.parseInt(bytesToString(bytes));
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
}
