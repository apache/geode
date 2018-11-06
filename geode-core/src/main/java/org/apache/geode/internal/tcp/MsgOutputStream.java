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
package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.ObjToByteArraySerializer;

/**
 * MsgOutputStream should no longer be used except in Connection to do the handshake. Otherwise
 * MsgStreamer should always be used.
 *
 * @since GemFire 3.0
 *
 */
public class MsgOutputStream extends OutputStream implements ObjToByteArraySerializer {
  private final ByteBuffer buffer;

  /**
   * The caller of this constructor is responsible for managing the allocated instance.
   */
  public MsgOutputStream(int allocSize) {
    if (TCPConduit.useDirectBuffers) {
      this.buffer = ByteBuffer.allocateDirect(allocSize);
    } else {
      this.buffer = ByteBuffer.allocate(allocSize);
    }
    this.buffer.position(Connection.MSG_HEADER_BYTES);
  }

  /** write the low-order 8 bits of the given int */
  @Override
  public void write(int b) {
    buffer.put((byte) b);
  }

  /** override OutputStream's write() */
  @Override
  public void write(byte[] source, int offset, int len) {
    this.buffer.put(source, offset, len);
  }

  private int size() {
    return this.buffer.position() - Connection.MSG_HEADER_BYTES;
  }

  /**
   * write the header after the message has been written to the stream
   */
  public void setMessageHeader(int msgType, int processorType, short msgId) {
    buffer.putInt(Connection.MSG_HEADER_SIZE_OFFSET, Connection.calcHdrSize(size()));
    buffer.put(Connection.MSG_HEADER_TYPE_OFFSET, (byte) (msgType & 0xff));
    buffer.putShort(Connection.MSG_HEADER_ID_OFFSET, msgId);
  }

  public void reset() {
    this.buffer.clear();
    this.buffer.position(Connection.MSG_HEADER_BYTES);
  }

  /**
   * gets the content ByteBuffer, ready for reading. The stream should not be written to past this
   * point until it has been reset.
   */
  public ByteBuffer getContentBuffer() {
    buffer.flip();
    return buffer;
  }

  // DataOutput methods
  /**
   * Writes a <code>boolean</code> value to this output stream. If the argument <code>v</code> is
   * <code>true</code>, the value <code>(byte)1</code> is written; if <code>v</code> is
   * <code>false</code>, the value <code>(byte)0</code> is written. The byte written by this method
   * may be read by the <code>readBoolean</code> method of interface <code>DataInput</code>, which
   * will then return a <code>boolean</code> equal to <code>v</code>.
   *
   * @param v the boolean to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  /**
   * Writes to the output stream the eight low- order bits of the argument <code>v</code>. The 24
   * high-order bits of <code>v</code> are ignored. (This means that <code>writeByte</code> does
   * exactly the same thing as <code>write</code> for an integer argument.) The byte written by this
   * method may be read by the <code>readByte</code> method of interface <code>DataInput</code>,
   * which will then return a <code>byte</code> equal to <code>(byte)v</code>.
   *
   * @param v the byte value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeByte(int v) throws IOException {
    write(v);
  }

  /**
   * Writes two bytes to the output stream to represent the value of the argument. The byte values
   * to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readShort</code> method of interface
   * <code>DataInput</code> , which will then return a <code>short</code> equal to
   * <code>(short)v</code>.
   *
   * @param v the <code>short</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeShort(int v) throws IOException {
    buffer.putShort((short) v);
  }

  /**
   * Writes a <code>char</code> value, wich is comprised of two bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readChar</code> method of interface
   * <code>DataInput</code> , which will then return a <code>char</code> equal to
   * <code>(char)v</code>.
   *
   * @param v the <code>char</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeChar(int v) throws IOException {
    buffer.putChar((char) v);
  }

  /**
   * Writes an <code>int</code> value, which is comprised of four bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 24))
   * (byte)(0xff &amp; (v &gt;&gt; 16))
   * (byte)(0xff &amp; (v &gt;&gt; &#32; &#32;8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readInt</code> method of interface
   * <code>DataInput</code> , which will then return an <code>int</code> equal to <code>v</code>.
   *
   * @param v the <code>int</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeInt(int v) throws IOException {
    buffer.putInt(v);
  }

  /**
   * Writes a <code>long</code> value, which is comprised of eight bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 56))
   * (byte)(0xff &amp; (v &gt;&gt; 48))
   * (byte)(0xff &amp; (v &gt;&gt; 40))
   * (byte)(0xff &amp; (v &gt;&gt; 32))
   * (byte)(0xff &amp; (v &gt;&gt; 24))
   * (byte)(0xff &amp; (v &gt;&gt; 16))
   * (byte)(0xff &amp; (v &gt;&gt;  8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readLong</code> method of interface
   * <code>DataInput</code> , which will then return a <code>long</code> equal to <code>v</code>.
   *
   * @param v the <code>long</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeLong(long v) throws IOException {
    buffer.putLong(v);
  }

  /**
   * Writes a <code>float</code> value, which is comprised of four bytes, to the output stream. It
   * does this as if it first converts this <code>float</code> value to an <code>int</code> in
   * exactly the manner of the <code>Float.floatToIntBits</code> method and then writes the
   * <code>int</code> value in exactly the manner of the <code>writeInt</code> method. The bytes
   * written by this method may be read by the <code>readFloat</code> method of interface
   * <code>DataInput</code>, which will then return a <code>float</code> equal to <code>v</code>.
   *
   * @param v the <code>float</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeFloat(float v) throws IOException {
    buffer.putFloat(v);
  }

  /**
   * Writes a <code>double</code> value, which is comprised of eight bytes, to the output stream. It
   * does this as if it first converts this <code>double</code> value to a <code>long</code> in
   * exactly the manner of the <code>Double.doubleToLongBits</code> method and then writes the
   * <code>long</code> value in exactly the manner of the <code>writeLong</code> method. The bytes
   * written by this method may be read by the <code>readDouble</code> method of interface
   * <code>DataInput</code>, which will then return a <code>double</code> equal to <code>v</code>.
   *
   * @param v the <code>double</code> value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeDouble(double v) throws IOException {
    buffer.putDouble(v);
  }

  /**
   * Writes a string to the output stream. For every character in the string <code>s</code>, taken
   * in order, one byte is written to the output stream. If <code>s</code> is <code>null</code>, a
   * <code>NullPointerException</code> is thrown.
   * <p>
   * If <code>s.length</code> is zero, then no bytes are written. Otherwise, the character
   * <code>s[0]</code> is written first, then <code>s[1]</code>, and so on; the last character
   * written is <code>s[s.length-1]</code>. For each character, one byte is written, the low-order
   * byte, in exactly the manner of the <code>writeByte</code> method . The high-order eight bits of
   * each character in the string are ignored.
   *
   * @param str the string of bytes to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeBytes(String str) throws IOException {
    int strlen = str.length();
    if (strlen > 0) {
      // I know this is a deprecated method but it is PERFECT for this impl.
      if (this.buffer.hasArray()) {
        // I know this is a deprecated method but it is PERFECT for this impl.
        int pos = this.buffer.position();
        str.getBytes(0, strlen, this.buffer.array(), this.buffer.arrayOffset() + pos);
        this.buffer.position(pos + strlen);
      } else {
        byte[] bytes = new byte[strlen];
        str.getBytes(0, strlen, bytes, 0);
        this.buffer.put(bytes);
      }
      // for (int i = 0 ; i < len ; i++) {
      // this.buffer.put((byte)s.charAt(i));
      // }
    }
  }

  /**
   * Writes every character in the string <code>s</code>, to the output stream, in order, two bytes
   * per character. If <code>s</code> is <code>null</code>, a <code>NullPointerException</code> is
   * thrown. If <code>s.length</code> is zero, then no characters are written. Otherwise, the
   * character <code>s[0]</code> is written first, then <code>s[1]</code>, and so on; the last
   * character written is <code>s[s.length-1]</code>. For each character, two bytes are actually
   * written, high-order byte first, in exactly the manner of the <code>writeChar</code> method.
   *
   * @param s the string value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeChars(String s) throws IOException {
    int len = s.length();
    if (len > 0) {
      for (int i = 0; i < len; i++) {
        this.buffer.putChar(s.charAt(i));
      }
    }
  }

  /**
   * Writes two bytes of length information to the output stream, followed by the Java modified UTF
   * representation of every character in the string <code>s</code>. If <code>s</code> is
   * <code>null</code>, a <code>NullPointerException</code> is thrown. Each character in the string
   * <code>s</code> is converted to a group of one, two, or three bytes, depending on the value of
   * the character.
   * <p>
   * If a character <code>c</code> is in the range <code>&#92;u0001</code> through
   * <code>&#92;u007f</code>, it is represented by one byte:
   * <p>
   *
   * <pre>
   * (byte) c
   * </pre>
   * <p>
   * If a character <code>c</code> is <code>&#92;u0000</code> or is in the range
   * <code>&#92;u0080</code> through <code>&#92;u07ff</code>, then it is represented by two bytes,
   * to be written in the order shown:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xc0 | (0x1f &amp; (c &gt;&gt; 6)))
   * (byte)(0x80 | (0x3f &amp; c))
   *  </code>
   * </pre>
   * <p>
   * If a character <code>c</code> is in the range <code>&#92;u0800</code> through
   * <code>uffff</code>, then it is represented by three bytes, to be written in the order shown:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xe0 | (0x0f &amp; (c &gt;&gt; 12)))
   * (byte)(0x80 | (0x3f &amp; (c &gt;&gt;  6)))
   * (byte)(0x80 | (0x3f &amp; c))
   *  </code>
   * </pre>
   * <p>
   * First, the total number of bytes needed to represent all the characters of <code>s</code> is
   * calculated. If this number is larger than <code>65535</code>, then a
   * <code>UTFDataFormatException</code> is thrown. Otherwise, this length is written to the output
   * stream in exactly the manner of the <code>writeShort</code> method; after this, the one-, two-,
   * or three-byte representation of each character in the string <code>s</code> is written.
   * <p>
   * The bytes written by this method may be read by the <code>readUTF</code> method of interface
   * <code>DataInput</code> , which will then return a <code>String</code> equal to <code>s</code>.
   *
   * @param str the string value to be written.
   * @exception IOException if an I/O error occurs.
   */
  public void writeUTF(String str) throws IOException {
    writeFullUTF(str);
  }

  private void writeFullUTF(String str) throws IOException {
    int strlen = str.length();
    if (strlen > 65535) {
      throw new UTFDataFormatException(
          "String too long for java serialization");
    }
    // make room for worst case space 3 bytes for each char and 2 for len
    int utfSizeIdx = this.buffer.position();
    // skip bytes reserved for length
    this.buffer.position(utfSizeIdx + 2);
    for (int i = 0; i < strlen; i++) {
      int c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        this.buffer.put((byte) c);
      } else if (c > 0x07FF) {
        this.buffer.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
        this.buffer.put((byte) (0x80 | ((c >> 6) & 0x3F)));
        this.buffer.put((byte) (0x80 | ((c >> 0) & 0x3F)));
      } else {
        this.buffer.put((byte) (0xC0 | ((c >> 6) & 0x1F)));
        this.buffer.put((byte) (0x80 | ((c >> 0) & 0x3F)));
      }
    }
    int utflen = this.buffer.position() - (utfSizeIdx + 2);
    if (utflen > 65535) {
      // act as if we wrote nothing to this buffer
      this.buffer.position(utfSizeIdx);
      throw new UTFDataFormatException(
          "String too long for java serialization");
    }
    this.buffer.putShort(utfSizeIdx, (short) utflen);
  }

  /**
   * Writes the given object to this stream as a byte array. The byte array is produced by
   * serializing v. The serialization is done by calling DataSerializer.writeObject.
   */
  public void writeAsSerializedByteArray(Object v) throws IOException {
    ByteBuffer sizeBuf = this.buffer;
    int sizePos = sizeBuf.position();
    sizeBuf.position(sizePos + 5);
    final int preArraySize = size();
    DataSerializer.writeObject(v, this);
    int arraySize = size() - preArraySize;
    sizeBuf.put(sizePos, InternalDataSerializer.INT_ARRAY_LEN);
    sizeBuf.putInt(sizePos + 1, arraySize);
  }
}
