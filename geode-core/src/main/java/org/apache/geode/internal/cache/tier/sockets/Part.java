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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.offheap.AddressableMemoryManager;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.Version;

/**
 * Represents one unit of information (essentially a <code>byte</code> array) in the wire protocol.
 * Each server connection runs in its own thread to maximize concurrency and improve response times
 * to edge requests
 *
 * @see Message
 * @since GemFire 2.0.2
 */
public class Part {

  private static final byte BYTE_CODE = 0;
  private static final byte OBJECT_CODE = 1;

  private Version version;

  /**
   * Used to represent and empty byte array for bug 36279
   *
   * @since GemFire 5.1
   */
  private static final byte EMPTY_BYTEARRAY_CODE = 2;
  @Immutable
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * The payload of this part. Could be null, a byte[] or a HeapDataOutputStream on the send side.
   * Could be null, or a byte[] on the receiver side.
   */
  private Object part;

  /** Is the payload (<code>part</code>) a serialized object? */
  private byte typeCode;

  public void init(byte[] v, byte tc) {
    if (tc == EMPTY_BYTEARRAY_CODE) {
      this.part = EMPTY_BYTE_ARRAY;
    } else {
      this.part = v;
    }
    this.typeCode = tc;
  }

  public void clear() {
    if (this.part != null) {
      if (this.part instanceof HeapDataOutputStream) {
        ((HeapDataOutputStream) this.part).close();
      }
      this.part = null;
    }
    this.typeCode = BYTE_CODE;
  }

  public boolean isNull() {
    if (this.part == null) {
      return true;
    }
    if (isObject() && this.part instanceof byte[]) {
      byte[] b = (byte[]) this.part;
      if (b.length == 1 && b[0] == DSCODE.NULL.toByte()) {
        return true;
      }
    }
    return false;
  }

  public boolean isObject() {
    return this.typeCode == OBJECT_CODE;
  }

  public boolean isBytes() {
    return this.typeCode == BYTE_CODE || this.typeCode == EMPTY_BYTEARRAY_CODE;
  }

  public void setPartState(byte[] b, boolean isObject) {
    if (isObject) {
      this.typeCode = OBJECT_CODE;
    } else if (b != null && b.length == 0) {
      this.typeCode = EMPTY_BYTEARRAY_CODE;
      b = EMPTY_BYTE_ARRAY;
    } else {
      this.typeCode = BYTE_CODE;
    }
    this.part = b;
  }

  public void setPartState(HeapDataOutputStream os, boolean isObject) {
    if (isObject) {
      this.typeCode = OBJECT_CODE;
      this.part = os;
    } else if (os != null && os.size() == 0) {
      this.typeCode = EMPTY_BYTEARRAY_CODE;
      this.part = EMPTY_BYTE_ARRAY;
    } else {
      this.typeCode = BYTE_CODE;
      this.part = os;
    }
  }

  public void setPartState(StoredObject so, boolean isObject) {
    if (isObject) {
      this.typeCode = OBJECT_CODE;
    } else if (so.getDataSize() == 0) {
      this.typeCode = EMPTY_BYTEARRAY_CODE;
      this.part = EMPTY_BYTE_ARRAY;
      return;
    } else {
      this.typeCode = BYTE_CODE;
    }
    if (so.hasRefCount()) {
      this.part = so;
    } else {
      this.part = so.getValueAsHeapByteArray();
    }
  }

  public byte getTypeCode() {
    return this.typeCode;
  }

  /**
   * Return the length of the part. The length is the number of bytes needed for its serialized
   * form.
   */
  public int getLength() {
    if (this.part == null) {
      return 0;
    } else if (this.part instanceof byte[]) {
      return ((byte[]) this.part).length;
    } else if (this.part instanceof StoredObject) {
      return ((StoredObject) this.part).getDataSize();
    } else {
      return ((HeapDataOutputStream) this.part).size();
    }
  }

  public String getString() {
    if (this.part == null) {
      return null;
    }
    if (!isBytes()) {
      Assert.assertTrue(false, "expected String part to be of type BYTE, part =" + this.toString());
    }
    return CacheServerHelper.fromUTF((byte[]) this.part);
  }

  @MakeNotStatic("not tied to the cache lifecycle")
  private static final Map<ByteArrayKey, String> CACHED_STRINGS = new ConcurrentHashMap<>();

  private static String getCachedString(byte[] serializedBytes) {
    ByteArrayKey key = new ByteArrayKey(serializedBytes);
    String result = CACHED_STRINGS.get(key);
    if (result == null) {
      result = CacheServerHelper.fromUTF(serializedBytes);
      CACHED_STRINGS.put(key, result);
    }
    return result;
  }

  /**
   * Used to wrap a byte array so that it can be used
   * as a key on a HashMap. This is needed so that
   * equals and hashCode will be based on the contents
   * of the byte array instead of the identity.
   */
  private static final class ByteArrayKey {
    private final byte[] bytes;

    public ByteArrayKey(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ByteArrayKey)) {
        return false;
      }
      ByteArrayKey other = (ByteArrayKey) obj;
      return Arrays.equals(bytes, other.bytes);
    }
  }

  /**
   * Like getString but will also check a cache of frequently serialized strings.
   * The result will be added to the cache if it is not already in it.
   * NOTE: only call this for strings that are reused often (like region names).
   */
  public String getCachedString() {
    if (this.part == null) {
      return null;
    }
    if (!isBytes()) {
      Assert.assertTrue(false, "expected String part to be of type BYTE, part =" + this.toString());
    }
    return getCachedString((byte[]) this.part);
  }

  @Immutable
  private static final byte[][] BYTES = new byte[256][1];
  private static final int BYTES_OFFSET = -1 * Byte.MIN_VALUE;
  static {
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      BYTES[i + BYTES_OFFSET][0] = i;
    }
  }

  public void setByte(byte b) {
    this.typeCode = BYTE_CODE;
    this.part = BYTES[b + BYTES_OFFSET];
  }

  public byte getByte() {
    if (!isBytes()) {
      Assert.assertTrue(false, "expected int part to be of type BYTE, part = " + this.toString());
    }
    if (getLength() != 1) {
      Assert.assertTrue(false,
          "expected int length to be 1 but it was " + getLength() + "; part = " + this.toString());
    }
    final byte[] bytes = getSerializedForm();
    return bytes[0];
  }

  public int getInt() {
    if (!isBytes()) {
      Assert.assertTrue(false, "expected int part to be of type BYTE, part = " + this.toString());
    }
    if (getLength() != 4) {
      Assert.assertTrue(false,
          "expected int length to be 4 but it was " + getLength() + "; part = " + this.toString());
    }
    byte[] bytes = getSerializedForm();
    return decodeInt(bytes, 0);
  }

  public static int decodeInt(byte[] bytes, int offset) {
    return (((bytes[offset + 0]) << 24) & 0xFF000000) | (((bytes[offset + 1]) << 16) & 0x00FF0000)
        | (((bytes[offset + 2]) << 8) & 0x0000FF00) | ((bytes[offset + 3]) & 0x000000FF);
  }

  @MakeNotStatic
  private static final Map<Integer, byte[]> CACHED_INTS = new ConcurrentHashMap<Integer, byte[]>();

  public void setInt(int v) {
    byte[] bytes = CACHED_INTS.get(v);
    if (bytes == null) {
      bytes = new byte[4];
      encodeInt(v, bytes);
      CACHED_INTS.put(v, bytes);
    }
    this.typeCode = BYTE_CODE;
    this.part = bytes;
  }

  /**
   * @since GemFire 5.7
   */
  public static void encodeInt(int v, byte[] bytes) {
    encodeInt(v, bytes, 0);
  }

  public static void encodeInt(int v, byte[] bytes, int offset) {
    // encode an int into the given byte array
    bytes[offset + 0] = (byte) ((v & 0xFF000000) >> 24);
    bytes[offset + 1] = (byte) ((v & 0x00FF0000) >> 16);
    bytes[offset + 2] = (byte) ((v & 0x0000FF00) >> 8);
    bytes[offset + 3] = (byte) (v & 0x000000FF);
  }

  public void setLong(long v) {
    byte[] bytes = new byte[8];
    bytes[0] = (byte) ((v & 0xFF00000000000000l) >> 56);
    bytes[1] = (byte) ((v & 0x00FF000000000000l) >> 48);
    bytes[2] = (byte) ((v & 0x0000FF0000000000l) >> 40);
    bytes[3] = (byte) ((v & 0x000000FF00000000l) >> 32);
    bytes[4] = (byte) ((v & 0x00000000FF000000l) >> 24);
    bytes[5] = (byte) ((v & 0x0000000000FF0000l) >> 16);
    bytes[6] = (byte) ((v & 0x000000000000FF00l) >> 8);
    bytes[7] = (byte) (v & 0xFF);
    this.typeCode = BYTE_CODE;
    this.part = bytes;
  }

  public long getLong() {
    if (!isBytes()) {
      Assert.assertTrue(false, "expected long part to be of type BYTE, part = " + this.toString());
    }
    if (getLength() != 8) {
      Assert.assertTrue(false,
          "expected long length to be 8 but it was " + getLength() + "; part = " + this.toString());
    }
    byte[] bytes = getSerializedForm();
    return ((((long) bytes[0]) << 56) & 0xFF00000000000000l)
        | ((((long) bytes[1]) << 48) & 0x00FF000000000000l)
        | ((((long) bytes[2]) << 40) & 0x0000FF0000000000l)
        | ((((long) bytes[3]) << 32) & 0x000000FF00000000l)
        | ((((long) bytes[4]) << 24) & 0x00000000FF000000l)
        | ((((long) bytes[5]) << 16) & 0x0000000000FF0000l)
        | ((((long) bytes[6]) << 8) & 0x000000000000FF00l) | (bytes[7] & 0x00000000000000FFl);
  }

  public byte[] getSerializedForm() {
    if (this.part == null) {
      return null;
    } else if (this.part instanceof byte[]) {
      return (byte[]) this.part;
    } else {
      return null; // should not be called on sender side?
    }
  }

  public Object getObject(boolean unzip) throws IOException, ClassNotFoundException {
    if (isBytes()) {
      return this.part;
    } else {
      if (this.version != null) {
        return CacheServerHelper.deserialize((byte[]) this.part, this.version, unzip);
      } else {
        return CacheServerHelper.deserialize((byte[]) this.part, unzip);
      }
    }
  }

  public Object getObject() throws IOException, ClassNotFoundException {
    return getObject(false);
  }

  public Object getStringOrObject() throws IOException, ClassNotFoundException {
    if (isObject()) {
      return getObject();
    } else {
      return getString();
    }
  }

  /**
   * Write the contents of this part to the specified output stream. This is only called for parts
   * that will not fit into the commBuffer so they need to be written directly to the stream. A
   * stream is used because the client is configured for old IO (instead of nio).
   *
   * @param buf the buffer to use if any data needs to be copied to one
   */
  public void writeTo(OutputStream out, ByteBuffer buf) throws IOException {
    if (getLength() > 0) {
      if (this.part instanceof byte[]) {
        byte[] bytes = (byte[]) this.part;
        out.write(bytes, 0, bytes.length);
      } else if (this.part instanceof StoredObject) {
        StoredObject so = (StoredObject) this.part;
        ByteBuffer sobb = so.createDirectByteBuffer();
        if (sobb != null) {
          HeapDataOutputStream.writeByteBufferToStream(out, buf, sobb);
        } else {
          int bytesToSend = so.getDataSize();
          long addr = so.getAddressForReadingData(0, bytesToSend);
          while (bytesToSend > 0) {
            if (buf.remaining() == 0) {
              HeapDataOutputStream.flushStream(out, buf);
            }
            buf.put(AddressableMemoryManager.readByte(addr));
            addr++;
            bytesToSend--;
          }
        }
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream) this.part;
        try {
          hdos.sendTo(out, buf);
        } finally {
          hdos.rewind();
        }
      }
    }
  }

  /**
   * Write the contents of this part to the specified byte buffer. Precondition: caller has already
   * checked the length of this part and it will fit into "buf".
   */
  public void writeTo(ByteBuffer buf) {
    if (getLength() > 0) {
      if (this.part instanceof byte[]) {
        buf.put((byte[]) this.part);
      } else if (this.part instanceof StoredObject) {
        StoredObject c = (StoredObject) this.part;
        ByteBuffer bb = c.createDirectByteBuffer();
        if (bb != null) {
          buf.put(bb);
        } else {
          int bytesToSend = c.getDataSize();
          long addr = c.getAddressForReadingData(0, bytesToSend);
          while (bytesToSend > 0) {
            buf.put(AddressableMemoryManager.readByte(addr));
            addr++;
            bytesToSend--;
          }
        }
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream) this.part;
        try {
          hdos.sendTo(buf);
        } finally {
          hdos.rewind();
        }
      }
    }
  }

  /**
   * Write the contents of this part to the specified socket channel using the specified byte
   * buffer. This is only called for parts that will not fit into the commBuffer so they need to be
   * written directly to the socket. Precondition: buf contains nothing that needs to be sent
   */
  public void writeTo(SocketChannel sc, ByteBuffer buf) throws IOException {
    if (getLength() > 0) {
      final int BUF_MAX = buf.capacity();
      if (this.part instanceof byte[]) {
        final byte[] bytes = (byte[]) this.part;
        int off = 0;
        int len = bytes.length;
        buf.clear();
        while (len > 0) {
          int bytesThisTime = len;
          if (bytesThisTime > BUF_MAX) {
            bytesThisTime = BUF_MAX;
          }
          buf.put(bytes, off, bytesThisTime);
          len -= bytesThisTime;
          off += bytesThisTime;
          buf.flip();
          while (buf.remaining() > 0) {
            sc.write(buf);
          }
          buf.clear();
        }
      } else if (this.part instanceof StoredObject) {
        // instead of copying the StoredObject to buf try to create a direct ByteBuffer and
        // just write it directly to the socket channel.
        StoredObject c = (StoredObject) this.part;
        ByteBuffer bb = c.createDirectByteBuffer();
        if (bb != null) {
          while (bb.remaining() > 0) {
            sc.write(bb);
          }
        } else {
          int len = c.getDataSize();
          long addr = c.getAddressForReadingData(0, len);
          buf.clear();
          while (len > 0) {
            int bytesThisTime = len;
            if (bytesThisTime > BUF_MAX) {
              bytesThisTime = BUF_MAX;
            }
            len -= bytesThisTime;
            while (bytesThisTime > 0) {
              buf.put(AddressableMemoryManager.readByte(addr));
              addr++;
              bytesThisTime--;
            }
            buf.flip();
            while (buf.remaining() > 0) {
              sc.write(buf);
            }
            buf.clear();
          }
        }
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream) this.part;
        try {
          hdos.sendTo(sc, buf);
        } finally {
          hdos.rewind();
        }
      }
    }
  }

  private static String typeCodeToString(byte c) {
    switch (c) {
      case BYTE_CODE:
        return "BYTE_CODE";
      case OBJECT_CODE:
        return "OBJECT_CODE";
      case EMPTY_BYTEARRAY_CODE:
        return "EMPTY_BYTEARRAY_CODE";
      default:
        return "unknown code " + c;
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("partCode=");
    sb.append(typeCodeToString(this.typeCode));
    sb.append(" partLength=" + getLength());
    return sb.toString();
  }

  public void setVersion(Version clientVersion) {
    this.version = clientVersion;
  }
}
