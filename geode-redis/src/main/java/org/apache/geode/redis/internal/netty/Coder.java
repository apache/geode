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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;

/**
 * This is a safe encoder and decoder for all redis matching needs
 */
public class Coder {

  /**
   * byte identifier of a bulk string
   */
  public static final byte BULK_STRING_ID = 36; // '$'

  /**
   * byte identifier of an array
   */
  public static final byte ARRAY_ID = 42; // '*'

  /**
   * byte identifier of an error
   */
  public static final byte ERROR_ID = 45; // '-'

  /**
   * byte identifier of an integer
   */
  public static final byte INTEGER_ID = 58; // ':'
  public static final byte NUMBER_1_BYTE = 0x31; // '1'
  /**
   * byte identifier of a simple string
   */
  public static final byte SIMPLE_STRING_ID = 43; // '+'
  public static final String CRLF = "\r\n";
  @MakeImmutable
  public static final byte[] CRLFar = stringToBytes(CRLF); // {13, 10} == {'\r', '\n'}

  /**
   * byte array of a nil response
   */
  @MakeImmutable
  public static final byte[] bNIL = stringToBytes("$-1\r\n"); // {'$', '-', '1', '\r', '\n'};

  /**
   * byte array of an empty array
   */
  @MakeImmutable
  public static final byte[] bEMPTY_ARRAY = stringToBytes("*0\r\n"); // {'*', '0', '\r', '\n'};

  /**
   * byte array of an empty string
   */
  @MakeImmutable
  public static final byte[] bEMPTY_STRING = stringToBytes("$0\r\n\r\n");

  @MakeImmutable
  public static final byte[] err = stringToBytes("ERR ");
  @MakeImmutable
  public static final byte[] wrongType = stringToBytes("WRONGTYPE ");

  /**
   * The charset being used by this coder, {@value #CHARSET}.
   */
  public static final String CHARSET = "UTF-8";

  @MakeImmutable
  protected static final DecimalFormat decimalFormatter = new DecimalFormat("#");

  static {
    decimalFormatter.setMaximumFractionDigits(10);
  }

  /**
   * Positive infinity string
   */
  public static final String P_INF = "+inf";

  /**
   * Negative infinity string
   */
  public static final String N_INF = "-inf";

  public static ByteBuf getBulkStringResponse(ByteBuf buffer, Object v)
      throws CoderException {
    byte[] toWrite;

    if (v == null) {
      buffer.writeBytes(bNIL);
    } else if (v instanceof byte[]) {
      toWrite = (byte[]) v;
      writeStringResponse(buffer, toWrite);
    } else if (v instanceof ByteArrayWrapper) {
      toWrite = ((ByteArrayWrapper) v).toBytes();
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
      buffer.writeBytes(CRLFar);
    } else if (v instanceof Long) {
      buffer.writeByte(INTEGER_ID);
      buffer.writeBytes(intToBytes(((Long) v).intValue()));
      buffer.writeBytes(CRLFar);
    } else {
      throw new CoderException();
    }

    return buffer;
  }

  private static void writeStringResponse(ByteBuf buffer, byte[] toWrite) {
    buffer.writeByte(BULK_STRING_ID);
    buffer.writeBytes(intToBytes(toWrite.length));
    buffer.writeBytes(CRLFar);
    buffer.writeBytes(toWrite);
    buffer.writeBytes(CRLFar);
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
    buffer.writeBytes(CRLFar);
    for (Object next : items) {
      writeCollectionOrString(buffer, next);
    }

    return buffer;
  }

  private static void writeCollectionOrString(ByteBuf buffer, Object next)
      throws CoderException {
    ByteBuf tmp = null;
    try {
      if (next instanceof Collection) {
        Collection<?> nextItems = (Collection<?>) next;
        getArrayResponse(buffer, nextItems);
      } else {
        getBulkStringResponse(buffer, next);
      }
    } finally {
      if (tmp != null) {
        tmp.release();
      }
    }
  }

  public static ByteBuf getScanResponse(ByteBuf buffer, BigInteger cursor,
      List<Object> scanResult) {
    buffer.writeByte(ARRAY_ID);
    buffer.writeBytes(intToBytes(2));
    buffer.writeBytes(CRLFar);
    buffer.writeByte(BULK_STRING_ID);
    byte[] cursorBytes = stringToBytes(cursor.toString());
    buffer.writeBytes(intToBytes(cursorBytes.length));
    buffer.writeBytes(CRLFar);
    buffer.writeBytes(cursorBytes);
    buffer.writeBytes(CRLFar);
    buffer.writeByte(ARRAY_ID);
    buffer.writeBytes(intToBytes(scanResult.size()));
    buffer.writeBytes(CRLFar);

    for (Object nextObject : scanResult) {
      if (nextObject instanceof String) {
        String next = (String) nextObject;
        buffer.writeByte(BULK_STRING_ID);
        buffer.writeBytes(intToBytes(next.length()));
        buffer.writeBytes(CRLFar);
        buffer.writeBytes(stringToBytes(next));
        buffer.writeBytes(CRLFar);
      } else if (nextObject instanceof ByteArrayWrapper) {
        byte[] next = ((ByteArrayWrapper) nextObject).toBytes();
        buffer.writeByte(BULK_STRING_ID);
        buffer.writeBytes(intToBytes(next.length));
        buffer.writeBytes(CRLFar);
        buffer.writeBytes(next);
        buffer.writeBytes(CRLFar);
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
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getErrorResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(err);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getCustomErrorResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getWrongTypeResponse(ByteBuf buffer, String error) {
    byte[] errorAr = stringToBytes(error);
    buffer.writeByte(ERROR_ID);
    buffer.writeBytes(wrongType);
    buffer.writeBytes(errorAr);
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, int integer) {
    buffer.writeByte(INTEGER_ID);
    buffer.writeBytes(intToBytes(integer));
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getIntegerResponse(ByteBuf buffer, long l) {
    buffer.writeByte(INTEGER_ID);
    buffer.writeBytes(longToBytes(l));
    buffer.writeBytes(CRLFar);
    return buffer;
  }

  public static ByteBuf getBigDecimalResponse(ByteBuf buffer, BigDecimal b) {
    writeStringResponse(buffer, bigDecimalToBytes(b));
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
    return new String(bytes);
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
    return string.getBytes();
  }

  /*
   * These toByte methods convert to byte arrays of the string representation of the input, not
   * literal to byte
   */

  public static byte[] intToBytes(int i) {
    // return stringToBytes(String.valueOf(i));
    return fastIntToBytes(i);
  }

  private static byte[] fastIntToBytes(int i) {
    if (i == Integer.MIN_VALUE) {
      return "-2147483648".getBytes();
    }

    int size = (i < 0) ? stringSize(-i) + 1 : stringSize(i);
    byte[] buffer = new byte[size];
    getChars(i, size, buffer);

    return buffer;
  }

  static void getChars(int i, int index, byte[] buffer) {
    int q, r;
    int charPos = index;
    byte sign = 0;

    if (i < 0) {
      sign = '-';
      i = -i;
    }

    // Generate two digits per iteration
    while (i >= 65536) {
      q = i / 100;
      // really: r = i - (q * 100);
      r = i - ((q << 6) + (q << 5) + (q << 2));
      i = q;
      buffer[--charPos] = DigitOnes[r];
      buffer[--charPos] = DigitTens[r];
    }

    // Fall thru to fast mode for smaller numbers
    // assert(i <= 65536, i);
    for (;;) {
      q = (i * 52429) >>> (16 + 3);
      r = i - ((q << 3) + (q << 1)); // r = i-(q*10) ...
      buffer[--charPos] = digits[r];
      i = q;
      if (i == 0)
        break;
    }
    if (sign != 0) {
      buffer[--charPos] = sign;
    }
  }

  static final byte[] digits = {
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
      0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
      0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
      0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e,
      0x6f, 0x70, 0x71, 0x72, 0x73, 0x74,
      0x75, 0x76, 0x77, 0x78, 0x79, 0x7a,
  };

  static final byte[] DigitOnes = {
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
  };

  static final byte[] DigitTens = {
      0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
      0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
      0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32, 0x32,
      0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
      0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34,
      0x35, 0x35, 0x35, 0x35, 0x35, 0x35, 0x35, 0x35, 0x35, 0x35,
      0x36, 0x36, 0x36, 0x36, 0x36, 0x36, 0x36, 0x36, 0x36, 0x36,
      0x37, 0x37, 0x37, 0x37, 0x37, 0x37, 0x37, 0x37, 0x37, 0x37,
      0x38, 0x38, 0x38, 0x38, 0x38, 0x38, 0x38, 0x38, 0x38, 0x38,
      0x39, 0x39, 0x39, 0x39, 0x39, 0x39, 0x39, 0x39, 0x39, 0x39,
  };

  static final int[] sizeTable = {9, 99, 999, 9999, 99999, 999999, 9999999,
      99999999, 999999999, Integer.MAX_VALUE};

  // Requires positive x
  static int stringSize(int x) {
    for (int i = 0;; i++)
      if (x <= sizeTable[i])
        return i + 1;
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
}
