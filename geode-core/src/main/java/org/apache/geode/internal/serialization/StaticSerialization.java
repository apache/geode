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
package org.apache.geode.internal.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.internal.MakeNotStatic;

public class StaticSerialization {
  // array is null
  public static final byte NULL_ARRAY = -1;
  /**
   * array len encoded as int in next 4 bytes
   *
   * @since GemFire 5.7
   */
  public static final byte INT_ARRAY_LEN = -3;

  public static final byte TIME_UNIT_NANOSECONDS = -1;
  public static final byte TIME_UNIT_MICROSECONDS = -2;
  public static final byte TIME_UNIT_MILLISECONDS = -3;
  public static final byte TIME_UNIT_SECONDS = -4;
  /**
   * array len encoded as unsigned short in next 2 bytes
   *
   * @since GemFire 5.7
   */
  public static final byte SHORT_ARRAY_LEN = -2;
  public static final int MAX_BYTE_ARRAY_LEN = (byte) -4 & 0xFF;
  // Variable Length long encoded as int in next 4 bytes
  public static final byte INT_VL = 126;
  // Variable Length long encoded as long in next 8 bytes
  public static final byte LONG_VL = 127;
  public static final int MAX_BYTE_VL = 125;

  @MakeNotStatic("not tied to the cache lifecycle")
  private static final ThreadLocalByteArrayCache threadLocalByteArrayCache =
      new ThreadLocalByteArrayCache(65535);

  public static void writeInetAddress(InetAddress address, DataOutput out) throws IOException {
    writeByteArray((address != null) ? address.getAddress() : null, out);
  }

  public static void writeByteArray(byte[] array, DataOutput out) throws IOException {
    int len = 0;
    if (array != null) {
      len = array.length;
    }
    writeByteArray(array, len, out);
  }

  public static void writeByteArray(byte[] array, int len, DataOutput out) throws IOException {

    int length = len; // to avoid warnings about parameter assignment

    if (array == null) {
      length = -1;
    } else {
      if (length > array.length) {
        length = array.length;
      }
    }
    writeArrayLength(length, out);
    if (length > 0) {
      out.write(array, 0, length);
    }
  }

  public static void writeArrayLength(int len, DataOutput out) throws IOException {
    if (len == -1) {
      out.writeByte(NULL_ARRAY);
    } else if (len <= MAX_BYTE_ARRAY_LEN) {
      out.writeByte(len);
    } else if (len <= 0xFFFF) {
      out.writeByte(SHORT_ARRAY_LEN);
      out.writeShort(len);
    } else {
      out.writeByte(INT_ARRAY_LEN);
      out.writeInt(len);
    }
  }

  public static int readArrayLength(DataInput in) throws IOException {
    byte code = in.readByte();
    if (code == NULL_ARRAY) {
      return -1;
    } else {
      int result = ubyteToInt(code);
      if (result > MAX_BYTE_ARRAY_LEN) {
        if (code == SHORT_ARRAY_LEN) {
          return in.readUnsignedShort();
        } else if (code == INT_ARRAY_LEN) {
          return in.readInt();
        } else {
          throw new IllegalStateException("unexpected array length code=" + code);
        }
      }
      return result;
    }
  }


  private static int ubyteToInt(byte ub) {
    return ub & 0xFF;
  }

  public static void writeString(String value, DataOutput out) throws IOException {

    if (value == null) {
      out.writeByte(DSCODE.NULL_STRING.toByte());

    } else {
      // writeUTF is expensive - it creates a char[] to fetch
      // the string's contents, iterates over the array to compute the
      // encoded length, creates a byte[] to hold the encoded bytes,
      // iterates over the char[] again to create the encode bytes,
      // then writes the bytes. Since we usually deal with ISO-8859-1
      // strings, we can accelerate this by accessing chars directly
      // with charAt and fill a single-byte buffer. If we run into
      // a multibyte char, we revert to using writeUTF()
      int len = value.length();
      int utfLen = len; // added for bug 40932
      for (int i = 0; i < len; i++) {
        char c = value.charAt(i);
        if ((c <= 0x007F) && (c >= 0x0001)) {
          // nothing needed
        } else if (c > 0x07FF) {
          utfLen += 2;
        } else {
          utfLen += 1;
        }
        // Note we no longer have an early out when we detect the first
        // non-ascii char because we need to compute the utfLen for bug 40932.
        // This is not a performance problem because most strings are ascii
        // and they never did the early out.
      }
      boolean writeUTF = utfLen > len;
      if (writeUTF) {
        if (utfLen > 0xFFFF) {
          out.writeByte(DSCODE.HUGE_STRING.toByte());
          out.writeInt(len);
          out.writeChars(value);
        } else {
          out.writeByte(DSCODE.STRING.toByte());
          out.writeUTF(value);
        }
      } else {
        if (len > 0xFFFF) {
          out.writeByte(DSCODE.HUGE_STRING_BYTES.toByte());
          out.writeInt(len);
          out.writeBytes(value);
        } else {
          out.writeByte(DSCODE.STRING_BYTES.toByte());
          out.writeShort(len);
          out.writeBytes(value);
        }
      }
    }
  }

  public static String readString(DataInput in, byte header) throws IOException {
    return readString(in, DscodeHelper.toDSCODE(header));
  }

  private static String readString(DataInput in, DSCODE dscode) throws IOException {
    switch (dscode) {
      case STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readUnsignedShort());
      case STRING:
        return readStringUTFFromDataInput(in);
      case NULL_STRING: {
        return null;
      }
      case HUGE_STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readInt());
      case HUGE_STRING:
        return readHugeStringFromDataInput(in);
      default:
        throw new IOException("Unknown String header " + dscode);
    }
  }

  private static String readHugeStringFromDataInput(DataInput in) throws IOException {
    int len = in.readInt();
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] = in.readChar();
    }
    return new String(buf);
  }

  public static String[] readStringArray(DataInput in) throws IOException {

    int length = readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      String[] array = new String[length];
      for (int i = 0; i < length; i++) {
        array[i] = readString(in);
      }
      return array;
    }
  }

  private static String readStringUTFFromDataInput(DataInput in) throws IOException {
    return in.readUTF();
  }



  private static String readStringBytesFromDataInput(DataInput dataInput, int len)
      throws IOException {
    if (len == 0) {
      return "";
    }
    byte[] buf = getThreadLocalByteArray(len);
    dataInput.readFully(buf, 0, len);
    return new String(buf, 0, 0, len); // intentionally using deprecated constructor
  }

  /**
   * Reads an instance of <code>String</code> from a <code>DataInput</code>. The return value may be
   * <code>null</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   *
   * @see #writeString
   */
  public static String readString(DataInput in) throws IOException {
    return readString(in, in.readByte());
  }

  public static InetAddress readInetAddress(DataInput in) throws IOException {

    byte[] address = readByteArray(in);
    if (address == null) {
      return null;
    }

    try {
      InetAddress addr = InetAddress.getByAddress(address);
      return addr;
    } catch (UnknownHostException ex) {
      throw new IOException("While reading an InetAddress", ex);
    }

  }

  public static void writeStringArray(String[] array, DataOutput out) throws IOException {
    int length;
    if (array == null) {
      length = -1;
    } else {
      length = array.length;
    }
    writeArrayLength(length, out);
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        writeString(array[i], out);
      }
    }
  }

  public static void writeInteger(Integer value, DataOutput out) throws IOException {
    out.writeInt(value);
  }

  public static <K, V> HashMap<K, V> readHashMap(DataInput in, SerializationContext context)
      throws IOException, ClassNotFoundException {

    int size = readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      HashMap<K, V> map = new HashMap<>(size);
      for (int i = 0; i < size; i++) {
        K key = (K) context.getDataSerializer().readObject(in);
        V value = (V) context.getDataSerializer().readObject(in);
        map.put(key, value);
      }

      return map;
    }
  }


  public static int[] readIntArray(DataInput in) throws IOException {
    int length = readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      int[] array = new int[length];
      for (int i = 0; i < length; i++) {
        array[i] = in.readInt();
      }
      return array;
    }
  }

  public static byte[] readByteArray(DataInput in) throws IOException {

    int length = readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      byte[] array = new byte[length];
      in.readFully(array, 0, length);
      return array;
    }
  }

  public static void writeIntArray(int[] array, DataOutput out) throws IOException {
    int length;
    if (array == null) {
      length = -1;
    } else {
      length = array.length;
    }
    writeArrayLength(length, out);

    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeInt(array[i]);
      }
    }
  }


  public static void writeHashMap(Map<?, ?> map, DataOutput out, SerializationContext context)
      throws IOException {
    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    writeArrayLength(size, out);
    if (size > 0) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        context.getDataSerializer().writeObject(entry.getKey(), out);
        context.getDataSerializer().writeObject(entry.getValue(), out);
      }
    }
  }



  /**
   * Returns a byte array for use by the calling thread.
   * The returned byte array may be longer than minimumLength.
   * The byte array belongs to the calling thread but callers must
   * be careful to not call other methods that may also use this
   * byte array.
   */
  public static byte[] getThreadLocalByteArray(int minimumLength) {
    return threadLocalByteArrayCache.get(minimumLength);
  }
}
