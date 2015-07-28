package com.gemstone.gemfire.internal.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.query.Struct;

/**
 * This is a safe encoder and decoder for all redis matching needs
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class Coder {

  /*
   * Take no chances on char to byte conversions with default charsets on jvms, 
   * so we'll hard code the UTF-8 symbol values as bytes here
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

  public static final byte OPEN_BRACE_ID = 0x28; // '('
  public static final byte OPEN_BRACKET_ID = 0x5b; // '['
  public static final byte HYPHEN_ID = 0x2d; // '-'
  public static final byte PLUS_ID = 0x2b; // '+'
  public static final byte NUMBER_1_BYTE = 0x31; // '1'
  /**
   * byte identifier of a simple string
   */
  public static final byte SIMPLE_STRING_ID = 43; // '+'
  public static final String CRLF = "\r\n";
  public static final byte[] CRLFar = stringToBytes(CRLF); // {13, 10} == {'\r', '\n'}

  /**
   * byte array of a nil response
   */
  public static final byte[] bNIL = stringToBytes("$-1\r\n"); // {'$', '-', '1', '\r', '\n'};

  /**
   * byte array of an empty string
   */
  public static final byte[] bEMPTY_ARRAY = stringToBytes("*0\r\n"); // {'*', '0', '\r', '\n'};

  public static final byte[] err = stringToBytes("ERR ");
  public static final byte[] noAuth = stringToBytes("NOAUTH ");
  public static final byte[] wrongType = stringToBytes("WRONGTYPE ");

  /**
   * The charset being used by this coder, {@value #CHARSET}.
   */
  public static final String CHARSET = "UTF-8";

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

  public static final ByteBuf getBulkStringResponse(ByteBufAllocator alloc, byte[] value) {
    ByteBuf response = alloc.buffer(value.length + 20);
    response.writeByte(BULK_STRING_ID);
    response.writeBytes(intToBytes(value.length));
    response.writeBytes(CRLFar);
    response.writeBytes(value);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getBulkStringResponse(ByteBufAllocator alloc, double value) {
    ByteBuf response = alloc.buffer();
    byte[] doub = doubleToBytes(value);
    response.writeByte(BULK_STRING_ID);
    response.writeBytes(intToBytes(doub.length));
    response.writeBytes(CRLFar);
    response.writeBytes(doub);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getBulkStringResponse(ByteBufAllocator alloc, String value) {
    byte[] valueAr = stringToBytes(value);
    int length = valueAr == null ? 0 : valueAr.length;
    ByteBuf response = alloc.buffer(length + 20);
    response.writeByte(BULK_STRING_ID);
    response.writeBytes(intToBytes(length));
    response.writeBytes(CRLFar);
    response.writeBytes(valueAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getBulkStringArrayResponse(ByteBufAllocator alloc, List<String> items) {
    Iterator<String> it = items.iterator();
    ByteBuf response = alloc.buffer();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(items.size()));
    response.writeBytes(CRLFar);
    while(it.hasNext()) {
      String next = it.next();
      response.writeByte(BULK_STRING_ID);
      response.writeBytes(intToBytes(next.length()));
      response.writeBytes(CRLFar);
      response.writeBytes(stringToBytes(next));
      response.writeBytes(CRLFar);
    }
    return response;
  }

  public static final ByteBuf getBulkStringArrayResponse(ByteBufAllocator alloc, Collection<ByteArrayWrapper> items) {
    Iterator<ByteArrayWrapper> it = items.iterator();
    ByteBuf response = alloc.buffer();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(items.size()));
    response.writeBytes(CRLFar);
    while(it.hasNext()) {
      ByteArrayWrapper nextWrapper = it.next();
      if (nextWrapper != null) {
        response.writeByte(BULK_STRING_ID);
        response.writeBytes(intToBytes(nextWrapper.length()));
        response.writeBytes(CRLFar);
        response.writeBytes(nextWrapper.toBytes());
        response.writeBytes(CRLFar);
      } else
        response.writeBytes(getNilResponse(alloc));
    }

    return response;
  }

  public static final ByteBuf getKeyValArrayResponse(ByteBufAllocator alloc, Collection<Entry<ByteArrayWrapper, ByteArrayWrapper>> items) {
    Iterator<Map.Entry<ByteArrayWrapper,ByteArrayWrapper>> it = items.iterator();
    ByteBuf response = alloc.buffer();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(items.size() * 2));
    response.writeBytes(CRLFar);

    try {
      while(it.hasNext()) {
        Map.Entry<ByteArrayWrapper,ByteArrayWrapper> next = it.next();
        byte[] key = next.getKey().toBytes();
        byte[] nextByteArray = next.getValue().toBytes();
        response.writeByte(BULK_STRING_ID); // Add key
        response.writeBytes(intToBytes(key.length));
        response.writeBytes(CRLFar);
        response.writeBytes(key);
        response.writeBytes(CRLFar);
        response.writeByte(BULK_STRING_ID); // Add value
        response.writeBytes(intToBytes(nextByteArray.length));
        response.writeBytes(CRLFar);
        response.writeBytes(nextByteArray);
        response.writeBytes(CRLFar);
      }
    } catch(Exception e) {
      return null;
    }

    return response;
  }

  public static final ByteBuf getScanResponse(ByteBufAllocator alloc, List<?> items) {
    ByteBuf response = alloc.buffer();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(2));
    response.writeBytes(CRLFar);
    response.writeByte(BULK_STRING_ID);
    byte[] cursor = stringToBytes((String) items.get(0));
    response.writeBytes(intToBytes(cursor.length));
    response.writeBytes(CRLFar);
    response.writeBytes(cursor);
    response.writeBytes(CRLFar);
    items = items.subList(1, items.size());
    Iterator<?> it = items.iterator();
    response.writeByte(ARRAY_ID);
    response.writeBytes(intToBytes(items.size()));
    response.writeBytes(CRLFar);

    try {
      while(it.hasNext()) {
        Object nextObject = it.next();
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
    } catch (Exception e) {
      return null;
    }
    return response;
  }

  public static final ByteBuf getEmptyArrayResponse(ByteBufAllocator alloc) {
    ByteBuf buf = alloc.buffer().writeBytes(bEMPTY_ARRAY);
    return buf;
  }

  public static final ByteBuf getSimpleStringResponse(ByteBufAllocator alloc, String string) {
    byte[] simpAr = stringToBytes(string);

    ByteBuf response = alloc.buffer(simpAr.length + 20);
    response.writeByte(SIMPLE_STRING_ID);
    response.writeBytes(simpAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getErrorResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 25);
    response.writeByte(ERROR_ID);
    response.writeBytes(err);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }
  
  public static final ByteBuf getNoAuthResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 25);
    response.writeByte(ERROR_ID);
    response.writeBytes(noAuth);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getWrongTypeResponse(ByteBufAllocator alloc, String error) {
    byte[] errorAr = stringToBytes(error);
    ByteBuf response = alloc.buffer(errorAr.length + 31);
    response.writeByte(ERROR_ID);
    response.writeBytes(wrongType);
    response.writeBytes(errorAr);
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getIntegerResponse(ByteBufAllocator alloc, int integer) {
    ByteBuf response = alloc.buffer(15);
    response.writeByte(INTEGER_ID);
    response.writeBytes(intToBytes(integer));
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getIntegerResponse(ByteBufAllocator alloc, long l) {
    ByteBuf response = alloc.buffer(25);
    response.writeByte(INTEGER_ID);
    response.writeBytes(longToBytes(l));
    response.writeBytes(CRLFar);
    return response;
  }

  public static final ByteBuf getNilResponse(ByteBufAllocator alloc) {
    ByteBuf buf = alloc.buffer().writeBytes(bNIL);
    return buf;
  }

  public static ByteBuf getBulkStringArrayResponseOfValues(ByteBufAllocator alloc, Collection<?> items) {
    Iterator<?> it = items.iterator();
    ByteBuf response = alloc.buffer();
    response.writeByte(Coder.ARRAY_ID);
    response.writeBytes(intToBytes(items.size()));
    response.writeBytes(Coder.CRLFar);

    while(it.hasNext()) {
      Object next = it.next();
      ByteArrayWrapper nextWrapper = null;
      if (next instanceof Entry)
        nextWrapper = (ByteArrayWrapper) ((Entry<?, ?>) next).getValue();
      else if (next instanceof Struct)
        nextWrapper = (ByteArrayWrapper) ((Struct) next).getFieldValues()[1];
      if (nextWrapper != null) {
        response.writeByte(Coder.BULK_STRING_ID);
        response.writeBytes(intToBytes(nextWrapper.length()));
        response.writeBytes(Coder.CRLFar);
        response.writeBytes(nextWrapper.toBytes());
        response.writeBytes(Coder.CRLFar);
      } else {
        response.writeBytes(Coder.bNIL);
      }
    }
    return response;
  }

  public static ByteBuf zRangeResponse(ByteBufAllocator alloc, Collection<?> list, boolean withScores) {
    if (list.isEmpty())
      return Coder.getEmptyArrayResponse(alloc);

    ByteBuf buffer = alloc.buffer();
    buffer.writeByte(Coder.ARRAY_ID);
    if (!withScores)
      buffer.writeBytes(intToBytes(list.size()));
    else
      buffer.writeBytes(intToBytes(2 * list.size()));
    buffer.writeBytes(Coder.CRLFar);

    try {
      for(Object entry: list) {
        ByteArrayWrapper key;
        DoubleWrapper score;
        if (entry instanceof Entry) {
          key = (ByteArrayWrapper) ((Entry<?, ?>) entry).getKey();
          score = (DoubleWrapper) ((Entry<?, ?>) entry).getValue();;
        } else {
          Object[] fieldVals = ((Struct) entry).getFieldValues();
          key = (ByteArrayWrapper) fieldVals[0];
          score = (DoubleWrapper) fieldVals[1];
        }
        byte[] byteAr = key.toBytes();
        buffer.writeByte(Coder.BULK_STRING_ID);
        buffer.writeBytes(intToBytes(byteAr.length));
        buffer.writeBytes(Coder.CRLFar);
        buffer.writeBytes(byteAr);
        buffer.writeBytes(Coder.CRLFar);
        if (withScores) {
          String scoreString = score.toString();
          byte[] scoreAr = stringToBytes(scoreString);
          buffer.writeByte(Coder.BULK_STRING_ID);
          buffer.writeBytes(intToBytes(scoreString.length()));
          buffer.writeBytes(Coder.CRLFar);
          buffer.writeBytes(scoreAr);
          buffer.writeBytes(Coder.CRLFar);
        }
      }
    } catch(Exception e) {
      return null;
    }
    return buffer;
  }

  public static ByteBuf getArrayOfNils(ByteBufAllocator alloc, int length) {
    ByteBuf response = alloc.buffer();
    response.writeByte(Coder.ARRAY_ID);
    response.writeBytes(intToBytes(length));
    response.writeBytes(Coder.CRLFar);

    for (int i = 0; i < length; i++)
      response.writeBytes(bNIL);

    return response;
  }

  public static String bytesToString(byte[] bytes) {
    if (bytes == null)
      return null;
    try {
      return new String(bytes, CHARSET).intern();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String doubleToString(double d) {
    if (d == Double.POSITIVE_INFINITY)
      return "Infinity";
    else if (d == Double.NEGATIVE_INFINITY)
      return "-Infinity";
    return String.valueOf(d);
  }

  public static byte[] stringToBytes(String string) {
    if (string == null || string.equals(""))
      return null;
    try {
      return string.getBytes(CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ByteArrayWrapper stringToByteArrayWrapper(String s) {
    return new ByteArrayWrapper(stringToBytes(s));
  }

  /*
   * These toByte methods convert to byte arrays of the
   * string representation of the input, not literal to byte
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

  public static int bytesToInt(byte[] bytes) {
    return Integer.parseInt(bytesToString(bytes));
  }

  public static long bytesToLong(byte[] bytes) {
    return Long.parseLong(bytesToString(bytes));
  }

  /**
   * A conversion where the byte array actually represents a string,
   * so it is converted as a string not as a literal double
   * @param bytes Array holding double
   * @return Parsed value
   * @throw NumberFormatException if bytes to string does not yield a convertable double
   */
  public static Double bytesToDouble(byte[] bytes) {
    return stringToDouble(bytesToString(bytes));
  }

  /**
   * Redis specific manner to parse floats
   * @param d String holding double
   * @return Value of string
   * @throws NumberFormatException if the double cannot be parsed
   */
  public static double stringToDouble(String d) {
    if (d.equalsIgnoreCase(P_INF))
      return Double.POSITIVE_INFINITY;
    else if (d.equalsIgnoreCase(N_INF))
      return Double.NEGATIVE_INFINITY;
    else
      return Double.parseDouble(d);
  }

  public static ByteArrayWrapper stringToByteWrapper(String s) {
    return new ByteArrayWrapper(stringToBytes(s));
  }

}
