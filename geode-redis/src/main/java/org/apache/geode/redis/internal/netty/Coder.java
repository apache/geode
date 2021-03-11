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
import io.netty.buffer.ByteBufAllocator;

import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;

/**
 * This is a safe encoder and decoder for all redis matching needs
 */
public class Coder {


  /*
   * Take no chances on char to byte conversions with default charsets on jvms, so we'll hard code
   * the UTF-8 symbol values as bytes here
   */


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
  public static final byte[] oom = stringToBytes("OOM ");

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

  public static ByteBuf getBulkStringResponse(ByteBufAllocator alloc, Object v)
      throws CoderException {
    ByteBuf response;
    byte[] toWrite;

    if (v == null) {
      response = alloc.buffer();
      response.writeBytes(bNIL);
    } else if (v instanceof byte[]) {
      byte[] value = (byte[]) v;
      response = alloc.buffer(value.length + 20);
      toWrite = value;
      writeStringResponse(response, toWrite);
    } else if (v instanceof ByteArrayWrapper) {
      byte[] value = ((ByteArrayWrapper) v).toBytes();
      response = alloc.buffer(value.length + 20);
      toWrite = value;
      writeStringResponse(response, toWrite);
    } else if (v instanceof Double) {
      response = alloc.buffer();
      toWrite = doubleToBytes(((Double) v).doubleValue());
      writeStringResponse(response, toWrite);
    } else if (v instanceof String) {
      String value = (String) v;
      response = alloc.buffer(value.length() + 20);
      toWrite = stringToBytes(value);
      writeStringResponse(response, toWrite);
    } else if (v instanceof Integer) {
      response = alloc.buffer(15);
      response.writeByte(INTEGER_ID);
      response.writeBytes(intToBytes((Integer) v));
      response.writeBytes(CRLFar);
    } else if (v instanceof Long) {
      response = alloc.buffer(15);
      response.writeByte(INTEGER_ID);
      response.writeBytes(intToBytes(((Long) v).intValue()));
      response.writeBytes(CRLFar);
    } else {
      throw new CoderException();
    }

    return response;
  }

  private static void writeStringResponse(ByteBuf response, byte[] toWrite) {
    response.writeByte(BULK_STRING_ID);
    response.writeBytes(intToBytes(toWrite.length));
    response.writeBytes(CRLFar);
    response.writeBytes(toWrite);
    response.writeBytes(CRLFar);
  }

  public static ByteBuf getFlattenedArrayResponse(ByteBufAllocator alloc,
      Collection<Collection<?>> items)
      throws CoderException {
    ByteBuf response = alloc.buffer();

    for (Object next : items) {
      writeCollectionOrString(alloc, response, next);
    }

    return response;
  }

  public static ByteBuf getArrayResponse(ByteBufAllocator alloc, Collection<?> items)
      throws CoderException {
    ByteBuf response = alloc.buffer();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(items.size()));
    response.writeBytes(CRLFar);
    for (Object next : items) {
      writeCollectionOrString(alloc, response, next);
    }

    return response;
  }

  private static void writeCollectionOrString(ByteBufAllocator alloc, ByteBuf response, Object next)
      throws CoderException {
    ByteBuf tmp = null;
    try {
      if (next instanceof Collection) {
        Collection<?> nextItems = (Collection<?>) next;
        tmp = getArrayResponse(alloc, nextItems);
        response.writeBytes(tmp);
      } else {
        tmp = getBulkStringResponse(alloc, next);
        response.writeBytes(tmp);
      }
    } finally {
      if (tmp != null) {
        tmp.release();
      }
    }
  }

  public static ByteBuf getScanResponse(ByteBufAllocator alloc, BigInteger cursor,
      List<Object> scanResult) {
    ByteBuf response = alloc.buffer();

    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(2));
    response.writeBytes(CRLFar);
    response.writeByte(BULK_STRING_ID);
    byte[] cursorBytes = stringToBytes(cursor.toString());
    response.writeBytes(intToBytes(cursorBytes.length));
    response.writeBytes(CRLFar);
    response.writeBytes(cursorBytes);
    response.writeBytes(CRLFar);
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(scanResult.size()));
    response.writeBytes(CRLFar);

    for (Object nextObject : scanResult) {
      if (nextObject instanceof String) {
        String next = (String) nextObject;
        response.writeByte(BULK_STRING_ID);
        response.writeBytes(intToBytes(next.length()));
        response.writeBytes(CRLFar);
        response.writeBytes(stringToBytes(next));
        response.writeBytes(CRLFar);
      } else if (nextObject instanceof ByteArrayWrapper) {
        byte[] next = ((ByteArrayWrapper) nextObject).toBytes();
        response.writeByte(BULK_STRING_ID);
        response.writeBytes(intToBytes(next.length));
        response.writeBytes(CRLFar);
        response.writeBytes(next);
        response.writeBytes(CRLFar);
      }
    }
    return response;
  }

  public static ByteBuf getEmptyArrayResponse(ByteBufAllocator alloc) {
    ByteBuf buf = alloc.buffer().writeBytes(bEMPTY_ARRAY);
    return buf;
  }

  public static ByteBuf getEmptyStringResponse(ByteBufAllocator alloc) {
    ByteBuf buf = alloc.buffer().writeBytes(bEMPTY_STRING);
    return buf;
  }

  public static ByteBuf getSimpleStringResponse(ByteBufAllocator alloc, String string) {
    byte[] simpAr = stringToBytes(string);
    return getSimpleStringResponse(alloc, simpAr);
  }

  public static ByteBuf getSimpleStringResponse(ByteBufAllocator alloc, byte[] byteArray) {
    ByteBuf response = alloc.buffer(byteArray.length + 20);
    response.writeByte(SIMPLE_STRING_ID);
    response.writeBytes(byteArray);
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getErrorResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 25);
    response.writeByte(ERROR_ID);
    response.writeBytes(err);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getOOMResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 25);
    response.writeByte(ERROR_ID);
    response.writeBytes(oom);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getCustomErrorResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 25);
    response.writeByte(ERROR_ID);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getWrongTypeResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 31);
    response.writeByte(ERROR_ID);
    response.writeBytes(wrongType);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getIntegerResponse(ByteBufAllocator alloc, int integer) {
    ByteBuf response = alloc.buffer(15);
    response.writeByte(INTEGER_ID);
    response.writeBytes(intToBytes(integer));
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getIntegerResponse(ByteBufAllocator alloc, long l) {
    ByteBuf response = alloc.buffer(25);
    response.writeByte(INTEGER_ID);
    response.writeBytes(longToBytes(l));
    response.writeBytes(CRLFar);
    return response;
  }

  public static ByteBuf getBigDecimalResponse(ByteBufAllocator alloc, BigDecimal b) {
    ByteBuf response = alloc.buffer();
    writeStringResponse(response, bigDecimalToBytes(b));
    return response;
  }

  public static ByteBuf getNilResponse(ByteBufAllocator alloc) {
    ByteBuf buf = alloc.buffer().writeBytes(bNIL);
    return buf;
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
