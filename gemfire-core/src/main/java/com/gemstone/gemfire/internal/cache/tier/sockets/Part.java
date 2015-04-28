/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.internal.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * Represents one unit of information (essentially a <code>byte</code>
 * array) in the wire protocol.  Each server connection runs in its
 * own thread to maximize concurrency and improve response times to
 * edge requests
 *
 * @see Message
 *
 * @author Sudhir Menon
 * @since 2.0.2
 */
public class Part {
  private static final byte BYTE_CODE = 0;
  private static final byte OBJECT_CODE = 1;
  
  private Version version;
  /**
   * Used to represent and empty byte array for bug 36279
   * @since 5.1
   */
  private static final byte EMPTY_BYTEARRAY_CODE = 2;

  /** The payload of this part.
   * Could be null, a byte[] or a HeapDataOutputStream on the send side.
   * Could be null, or a byte[] on the receiver side.
   */
  private Object part;

  /** Is the payload (<code>part</code>) a serialized object? */
  private byte typeCode;

  public void init(byte[] v, byte tc) {
    if (tc == EMPTY_BYTEARRAY_CODE && v == null) {
      this.part = new byte[0];
    }
    else {
      this.part = v;
    }
    this.typeCode = tc;
  }

//   public void init(HeapDataOutputStream os, byte typeCode) {
//     this.part = os;
//     this.typeCode = typeCode;
//   }

  public void clear() {
    this.part = null;
    this.typeCode = BYTE_CODE;
  }

  public boolean isNull() {
    if (this.part == null) {
      return true;
    }
    if (isObject() && this.part instanceof byte[]) {
      byte[] b = (byte[])this.part;
      if (b.length == 1 && b[0] == DSCODE.NULL) {
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
//   public boolean isString() {
//     return this.typeCode == STRING_CODE;
//   }
  public void setPartState(byte[] b, boolean isObject) {
    if (isObject) {
      this.typeCode = OBJECT_CODE;
    } else if (b != null && b.length == 0) {
      this.typeCode = EMPTY_BYTEARRAY_CODE;
    } else {
      this.typeCode = BYTE_CODE;
    }
    this.part = b;
  }
  public void setPartState(HeapDataOutputStream os, boolean isObject) {
    if (isObject) {
      this.typeCode = OBJECT_CODE;
    } else if (os != null && os.size() == 0) {
      this.typeCode = EMPTY_BYTEARRAY_CODE;
    } else {
      this.typeCode = BYTE_CODE;
    }
    this.part = os;
  }
  public byte getTypeCode() {
    return this.typeCode;
  }
  /**
   * Return the length of the part. The length is the number of bytes needed
   * for its serialized form.
   */
  public int getLength() {
    if (this.part == null) {
      return 0;
    } else if (this.part instanceof byte[]) {
      return ((byte[])this.part).length;
    } else {
      return ((HeapDataOutputStream)this.part).size();
    }
  }
  public String getString() {
    if (this.part == null) {
      return null;
    }
    if (!isBytes()) {
      Assert.assertTrue(false, "expected String part to be of type BYTE, part ="
          + this.toString());
    }
    return CacheServerHelper.fromUTF((byte[])this.part);
  }
  
  public int getInt() {
    if (!isBytes()) {
      Assert.assertTrue(false, "expected int part to be of type BYTE, part = "
          + this.toString()); 
    }
    if (getLength() != 4) {
      Assert.assertTrue(false, 
          "expected int length to be 4 but it was " + getLength()
          + "; part = " + this.toString());
    }
    byte[] bytes = getSerializedForm();
    return decodeInt(bytes, 0);
  }

  public static int decodeInt(byte[] bytes, int offset) {
    return (((bytes[offset + 0]) << 24) & 0xFF000000)
        | (((bytes[offset + 1]) << 16) & 0x00FF0000)
        | (((bytes[offset + 2]) << 8) & 0x0000FF00)
        | ((bytes[offset + 3]) & 0x000000FF);
  }

  public void setInt(int v) {
    byte[] bytes = new byte[4];
    encodeInt(v, bytes);
    this.typeCode = BYTE_CODE;
    this.part = bytes;
  }

  /**
   * @since 5.7
   */
  public static void encodeInt(int v, byte[] bytes) {
    encodeInt(v, bytes, 0);
  }

  public static void encodeInt(int v, byte[] bytes, int offset) {
    // encode an int into the given byte array
    bytes[offset + 0] = (byte) ((v & 0xFF000000) >> 24);
    bytes[offset + 1] = (byte) ((v & 0x00FF0000) >> 16);
    bytes[offset + 2] = (byte) ((v & 0x0000FF00) >> 8 );
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
    bytes[6] = (byte) ((v & 0x000000000000FF00l) >>  8);
    bytes[7] = (byte) (v & 0xFF);
    this.typeCode = BYTE_CODE;
    this.part = bytes;
  }

  public long getLong() {
    if (!isBytes()) {
      Assert.assertTrue(false, "expected long part to be of type BYTE, part = "
          + this.toString()); 
    }
    if (getLength() != 8) {
      Assert.assertTrue(false, 
          "expected long length to be 8 but it was " + getLength()
          + "; part = " + this.toString());
    }
    byte[] bytes = getSerializedForm();
    return ((((long)bytes[0]) << 56) & 0xFF00000000000000l) |
           ((((long)bytes[1]) << 48) & 0x00FF000000000000l) |
           ((((long)bytes[2]) << 40) & 0x0000FF0000000000l) |
           ((((long)bytes[3]) << 32) & 0x000000FF00000000l) |
           ((((long)bytes[4]) << 24) & 0x00000000FF000000l) |
           ((((long)bytes[5]) << 16) & 0x0000000000FF0000l) |
           ((((long)bytes[6]) <<  8) & 0x000000000000FF00l) |
           (        bytes[7]         & 0x00000000000000FFl);
  }


  public byte[] getSerializedForm() {
    if (this.part == null) {
      return null;
    } else if (this.part instanceof byte[]) {
      return (byte[])this.part;
    } else {
      return null; // should not be called on sender side?
    }
  }
  public Object getObject(boolean unzip) throws IOException, ClassNotFoundException {
    if (isBytes()) {
      return this.part;
    }
    else {
      if (this.version != null) {
        return CacheServerHelper.deserialize((byte[])this.part, this.version,
            unzip);
      }
      else {
        return CacheServerHelper.deserialize((byte[])this.part, unzip);
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
   * Write the contents of this part to the specified output stream.
   */
  public final void sendTo(OutputStream out) throws IOException {
    if (getLength() > 0) {
      if (this.part instanceof byte[]) {
        byte[] bytes = (byte[])this.part;
        out.write(bytes, 0, bytes.length);
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream)this.part;
        hdos.sendTo(out);
        hdos.rewind();
      }
    }
  }
  /**
   * Write the contents of this part to the specified byte buffer.
   */
  public final void sendTo(ByteBuffer buf) {
    if (getLength() > 0) {
      if (this.part instanceof byte[]) {
        buf.put((byte[])this.part);
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream)this.part;
        hdos.sendTo(buf);
        hdos.rewind();
      }
    }
  }
  /**
   * Write the contents of this part to the specified socket channel
   * using the specified byte buffer.
   */
  public final void sendTo(SocketChannel sc, ByteBuffer buf) throws IOException {
    if (getLength() > 0) {
      final int BUF_MAX = buf.capacity();
      if (this.part instanceof byte[]) {
        final byte[] bytes = (byte[])this.part;
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
      } else {
        HeapDataOutputStream hdos = (HeapDataOutputStream)this.part;
        hdos.sendTo(sc, buf);
        hdos.rewind();
      }
    }
  }
  
  static private String typeCodeToString(byte c) {
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
//    sb.append(" partBytes=");
//    byte[] b = getSerializedForm();
//    if (b == null) {
//      sb.append("null");
//    }
//    else {
//      sb.append("(");
//      for (int i = 0; i < b.length; i ++) {
//        sb.append(Integer.toString(b[i]));
//        sb.append(" ");
//      }
//      sb.append(")");
//    }
    return sb.toString();
  }

  public void setVersion(Version clientVersion) {
    this.version = clientVersion;
  }
}
