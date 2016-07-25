/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataStream;

/**
 * A reusable {@link DataInput} implementation that wraps a given byte array. It
 * also implements {@link VersionedDataStream} for a stream coming from a
 * different product version.
 * 
 * @since GemFire 7.1
 */
public class ByteArrayDataInput extends InputStream implements DataInput,
    VersionedDataStream {

  private byte[] bytes;
  private int nBytes;
  private int pos;
  /** reusable buffer for readUTF */
  private char[] charBuf;
  private Version version;

  /**
   * Create a {@link DataInput} whose contents are empty.
   */
  public ByteArrayDataInput() {
  }

  /**
   * Initialize this byte array stream with given byte array and version.
   * 
   * @param bytes
   *          the content of this stream. Note that this byte array will be read
   *          by this class (a copy is not made) so it should not be changed
   *          externally.
   * @param version
   *          the product version that serialized the object on given bytes
   */
  public final void initialize(byte[] bytes, Version version) {
    this.bytes = bytes;
    this.nBytes = bytes.length;
    this.pos = 0;
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Version getVersion() {
    return this.version;
  }

  private final int skipOver(long n) {
    final int capacity = (this.nBytes - this.pos);
    if (n <= capacity) {
      this.pos += (int)n;
      return (int)n;
    }
    else {
      this.pos += capacity;
      return capacity;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read() throws IOException {
    if (this.pos < this.nBytes) {
      return (this.bytes[this.pos++] & 0xff);
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    else if (off < 0 || len < 0 || b.length < (off + len)) {
      throw new IndexOutOfBoundsException();
    }

    final int capacity = (this.nBytes - this.pos);
    if (len > capacity) {
      len = capacity;
    }
    if (len > 0) {
      System.arraycopy(this.bytes, this.pos, b, off, len);
      this.pos += len;
      return len;
    }
    else {
      return 0;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final long skip(long n) {
    return skipOver(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int available() {
    return (this.nBytes - this.pos);
  }

  /**
   * Get the current position in the byte[].
   */
  public final int position() {
    return this.pos;
  }

  /**
   * Set the current position in the byte[].
   */
  public final void setPosition(int pos) {
    this.pos = pos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void readFully(byte[] b) throws IOException {
    final int len = b.length;
    System.arraycopy(this.bytes, this.pos, b, 0, len);
    this.pos += len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void readFully(byte[] b, int off, int len) throws IOException {
    if (len > 0) {
      if ((this.nBytes - this.pos) >= len) {
        System.arraycopy(this.bytes, this.pos, b, off, len);
        this.pos += len;
      }
      else {
        throw new EOFException();
      }
    }
    else if (len < 0) {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int skipBytes(int n) {
    return skipOver(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean readBoolean() throws IOException {
    if (this.pos < this.nBytes) {
      return (this.bytes[this.pos++] != 0);
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final byte readByte() throws IOException {
    if (this.pos < this.nBytes) {
      return this.bytes[this.pos++];
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int readUnsignedByte() throws IOException {
    return read();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final short readShort() throws IOException {
    if ((this.pos + 1) < this.nBytes) {
      int result = (this.bytes[this.pos++] & 0xff);
      return (short)((result << 8) | (this.bytes[this.pos++] & 0xff));
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int readUnsignedShort() throws IOException {
    if ((this.pos + 1) < this.nBytes) {
      int result = (this.bytes[this.pos++] & 0xff);
      return ((result << 8) | (this.bytes[this.pos++] & 0xff));
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public char readChar() throws IOException {
    if ((this.pos + 1) < this.nBytes) {
      int result = this.bytes[this.pos++] << 8;
      return (char)(result | (this.bytes[this.pos++] & 0xff));
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readInt() throws IOException {
    if ((this.pos + 3) < this.nBytes) {
      int result = (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      return ((result << 8) | (this.bytes[this.pos++] & 0xff));
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final long readLong() throws IOException {
    if ((this.pos + 7) < this.nBytes) {
      long result = (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      result = (result << 8) | (this.bytes[this.pos++] & 0xff);
      return ((result << 8) | (this.bytes[this.pos++] & 0xff));
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final String readUTF() throws IOException {
    final int utfLen = readUnsignedShort();

    if ((this.pos + utfLen) <= this.nBytes) {
      if (this.charBuf == null || this.charBuf.length < utfLen) {
        int charBufLength = (((utfLen / 2) + 1) * 3);
        this.charBuf = new char[charBufLength];
      }
      final byte[] bytes = this.bytes;
      final char[] chars = this.charBuf;

      int index = this.pos;
      final int limit = index + utfLen;
      int nChars = 0;
      int char1, char2, char3;

      // quick check for ASCII strings first
      for (; index < limit; index++, nChars++) {
        char1 = (bytes[index] & 0xff);
        if (char1 < 128) {
          chars[nChars] = (char)char1;
          continue;
        }
        else {
          break;
        }
      }

      for (; index < limit; index++, nChars++) {
        char1 = (bytes[index] & 0xff);
        // classify based on the high order 3 bits
        switch (char1 >> 5) {
          case 6:
            if ((index + 1) < limit) {
              // two byte encoding
              // 110yyyyy 10xxxxxx
              // use low order 6 bits of the next byte
              // It should have high order bits 10.
              char2 = bytes[++index];
              if ((char2 & 0xc0) == 0x80) {
                // 00000yyy yyxxxxxx
                chars[nChars] = (char)((char1 & 0x1f) << 6 | (char2 & 0x3f));
              }
              else {
                throwUTFEncodingError(index, char1, char2, null, 2);
              }
            }
            else {
              throw new UTFDataFormatException(
                  "partial 2-byte character at end (char1=" + char1 + ')');
            }
            break;
          case 7:
            if ((index + 2) < limit) {
              // three byte encoding
              // 1110zzzz 10yyyyyy 10xxxxxx
              // use low order 6 bits of the next byte
              // It should have high order bits 10.
              char2 = bytes[++index];
              if ((char2 & 0xc0) == 0x80) {
                // use low order 6 bits of the next byte
                // It should have high order bits 10.
                char3 = bytes[++index];
                if ((char3 & 0xc0) == 0x80) {
                  // zzzzyyyy yyxxxxxx
                  chars[nChars] = (char)(((char1 & 0x0f) << 12)
                      | ((char2 & 0x3f) << 6) | (char3 & 0x3f));
                }
                else {
                  throwUTFEncodingError(index, char1, char2, char3, 3);
                }
              }
              else {
                throwUTFEncodingError(index, char1, char2, null, 3);
              }
            }
            else {
              throw new UTFDataFormatException(
                  "partial 3-byte character at end (char1=" + char1 + ')');
            }
            break;
          default:
            // one byte encoding
            // 0xxxxxxx
            chars[nChars] = (char)char1;
            break;
        }
      }
      this.pos = limit;
      return new String(chars, 0, nChars);
    }
    else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    this.bytes = null;
    this.nBytes = 0;
    this.pos = 0;
    this.version = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return this.version == null ? super.toString() : (super.toString() + " ("
        + this.version + ')');
  }

  private void throwUTFEncodingError(int index, int char1, int char2,
      Integer char3, int enc) throws UTFDataFormatException {
    throw new UTFDataFormatException("malformed input for " + enc
        + "-byte encoding at " + index + " (char1=" + char1 + " char2=" + char2
        + (char3 == null ? ")" : (" char3=" + char3 + ')')));
  }
}
