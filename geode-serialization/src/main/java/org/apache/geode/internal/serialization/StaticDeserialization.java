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

import static org.apache.geode.internal.serialization.StaticSerialization.INT_ARRAY_LEN;
import static org.apache.geode.internal.serialization.StaticSerialization.MAX_BYTE_ARRAY_LEN;
import static org.apache.geode.internal.serialization.StaticSerialization.NULL_ARRAY;
import static org.apache.geode.internal.serialization.StaticSerialization.POST_GEODE_100_TCPSERVER_PACKAGE;
import static org.apache.geode.internal.serialization.StaticSerialization.PRE_GEODE_100_TCPSERVER_PACKAGE;
import static org.apache.geode.internal.serialization.StaticSerialization.SHORT_ARRAY_LEN;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * StaticSerialization provides a collection of deserialization methods that
 * can be used in your fromData methods in a DataSerializableFixedID implementation.
 */
public class StaticDeserialization {

  @MakeNotStatic("not tied to the cache lifecycle")
  private static final ThreadLocalByteArrayCache threadLocalByteArrayCache =
      new ThreadLocalByteArrayCache(65535);

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

  public static String readString(DataInput in, byte header) throws IOException {
    return readString(in, DscodeHelper.toDSCODE(header));
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

  /**
   * Reads an instance of <code>String</code> from a <code>DataInput</code>. The return value may be
   * <code>null</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   *
   * @see StaticSerialization#writeString
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
      // note: this does not throw UnknownHostException at this time
      return InetAddress.getByAddress(address);
    } catch (UnknownHostException ex) {
      throw new IOException("While reading an InetAddress", ex);
    }

  }

  public static <K, V> HashMap<K, V> readHashMap(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {

    int size = readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      HashMap<K, V> map = new HashMap<>(size);
      for (int i = 0; i < size; i++) {
        K key = context.getDeserializer().readObject(in);
        V value = context.getDeserializer().readObject(in);
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

  public static Class<?> readClass(DataInput in) throws IOException, ClassNotFoundException {
    byte typeCode = in.readByte();
    if (typeCode == DSCODE.CLASS.toByte()) {
      String className = readString(in);
      className = processIncomingClassName(className);
      return Class.forName(className);
    } else {
      return decodePrimitiveClass(typeCode);
    }
  }

  /**
   * Map from old package to new package.
   *
   * @return the same name String (identity) if the package name does not need to change
   */
  public static String processIncomingClassName(String name) {
    // TCPServer classes are used before a cache exists and support for old clients has been
    // initialized
    if (name.startsWith(PRE_GEODE_100_TCPSERVER_PACKAGE)) {
      return POST_GEODE_100_TCPSERVER_PACKAGE
          + name.substring(PRE_GEODE_100_TCPSERVER_PACKAGE.length());
    }
    return name;
  }

  public static Class<?> decodePrimitiveClass(byte typeCode) throws IOException {
    DSCODE dscode = DscodeHelper.toDSCODE(typeCode);
    switch (dscode) {
      case BOOLEAN_TYPE:
        return Boolean.TYPE;
      case CHARACTER_TYPE:
        return Character.TYPE;
      case BYTE_TYPE:
        return Byte.TYPE;
      case SHORT_TYPE:
        return Short.TYPE;
      case INTEGER_TYPE:
        return Integer.TYPE;
      case LONG_TYPE:
        return Long.TYPE;
      case FLOAT_TYPE:
        return Float.TYPE;
      case DOUBLE_TYPE:
        return Double.TYPE;
      case VOID_TYPE:
        return Void.TYPE;
      case NULL:
        return null;
      default:
        throw new IllegalArgumentException(
            String.format("unexpected typeCode: %s", typeCode));
    }
  }

  /**
   * Get the {@link KnownVersion} of the peer or disk store that created this
   * {@link DataInput}. Returns
   * null if the version is same as this member's.
   */
  public static KnownVersion getVersionForDataStreamOrNull(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      return ((VersionedDataStream) in).getVersion();
    } else {
      // assume latest version
      return null;
    }
  }

  /**
   * Get the {@link KnownVersion} of the peer or disk store that created this
   * {@link DataInput}.
   */
  public static KnownVersion getVersionForDataStream(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      final KnownVersion v = ((VersionedDataStream) in).getVersion();
      return v != null ? v : KnownVersion.CURRENT;
    } else {
      // assume latest version
      return KnownVersion.CURRENT;
    }
  }

  private static int ubyteToInt(byte ub) {
    return ub & 0xFF;
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
    @SuppressWarnings("deprecation") // intentionally using deprecated constructor
    final String string = new String(buf, 0, 0, len);
    return string;
  }
}
