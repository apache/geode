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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

/**
 * A reusable {@link DataInput} implementation that wraps a given byte array. It also implements
 * {@link org.apache.geode.internal.serialization.VersionedDataStream} for a stream coming from a
 * different product version.
 */
public class ByteArrayDataInput extends InputStream implements DataInput, VersionedDataStream {

  private byte[] bytes;
  private int nBytes;
  private int pos;
  /** reusable buffer for readUTF */
  private char[] charBuf;
  private Version version;

  /**
   * Create a {@link DataInput} whose contents are empty.
   */
  public ByteArrayDataInput() {}

  public ByteArrayDataInput(byte[] bytes) {
    initialize(bytes, null);
  }

  public ByteArrayDataInput(byte[] bytes, Version version) {
    initialize(bytes, version);
  }

  /**
   * Initialize this byte array stream with given byte array and version.
   *
   * @param bytes the content of this stream. Note that this byte array will be read by this class
   *        (a copy is not made) so it should not be changed externally.
   * @param version the product version that serialized the object on given bytes
   */
  public void initialize(byte[] bytes, Version version) {
    this.bytes = bytes;
    nBytes = bytes.length;
    pos = 0;
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Version getVersion() {
    return version;
  }

  private int skipOver(long n) {
    final int capacity = (nBytes - pos);
    if (n <= capacity) {
      pos += (int) n;
      return (int) n;
    } else {
      pos += capacity;
      return capacity;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    if (pos < nBytes) {
      return (bytes[pos++] & 0xff);
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || b.length < (off + len)) {
      throw new IndexOutOfBoundsException();
    }

    final int capacity = (nBytes - pos);
    if (len > capacity) {
      len = capacity;
    }
    if (len > 0) {
      System.arraycopy(bytes, pos, b, off, len);
      pos += len;
      return len;
    } else {
      return 0;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long skip(long n) {
    return skipOver(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int available() {
    return (nBytes - pos);
  }

  /**
   * Get the current position in the byte[].
   */
  public int position() {
    return pos;
  }

  /**
   * Set the current position in the byte[].
   */
  public void setPosition(int pos) {
    this.pos = pos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    if (len > 0) {
      if ((nBytes - pos) >= len) {
        System.arraycopy(bytes, pos, b, off, len);
        pos += len;
      } else {
        throw new EOFException();
      }
    } else if (len < 0) {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int skipBytes(int n) {
    return skipOver(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean readBoolean() throws IOException {
    if (pos < nBytes) {
      return (bytes[pos++] != 0);
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte readByte() throws IOException {
    if (pos < nBytes) {
      return bytes[pos++];
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readUnsignedByte() throws IOException {
    return read();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public short readShort() throws IOException {
    if ((pos + 1) < nBytes) {
      int result = (bytes[pos++] & 0xff);
      return (short) ((result << 8) | (bytes[pos++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readUnsignedShort() throws IOException {
    if ((pos + 1) < nBytes) {
      int result = (bytes[pos++] & 0xff);
      return ((result << 8) | (bytes[pos++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public char readChar() throws IOException {
    if ((pos + 1) < nBytes) {
      int result = bytes[pos++] << 8;
      return (char) (result | (bytes[pos++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readInt() throws IOException {
    if ((pos + 3) < nBytes) {
      int result = (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      return ((result << 8) | (bytes[pos++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long readLong() throws IOException {
    if ((pos + 7) < nBytes) {
      long result = (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      result = (result << 8) | (bytes[pos++] & 0xff);
      return ((result << 8) | (bytes[pos++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readUTF() throws IOException {
    final int utfLen = readUnsignedShort();
    if (utfLen == 0) {
      return "";
    }

    if ((pos + utfLen) <= nBytes) {
      String asciiString = readASCII(utfLen);
      if (asciiString != null) {
        return asciiString;
      }
      if (charBuf == null || charBuf.length < utfLen) {
        int charBufLength = (((utfLen / 2) + 1) * 3);
        charBuf = new char[charBufLength];
      }
      final byte[] bytes = this.bytes;
      final char[] chars = charBuf;

      int index = pos;
      final int limit = index + utfLen;
      int nChars = 0;
      int char1, char2, char3;

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
                chars[nChars] = (char) ((char1 & 0x1f) << 6 | (char2 & 0x3f));
              } else {
                throwUTFEncodingError(index, char1, char2, null, 2);
              }
            } else {
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
                  chars[nChars] =
                      (char) (((char1 & 0x0f) << 12) | ((char2 & 0x3f) << 6) | (char3 & 0x3f));
                } else {
                  throwUTFEncodingError(index, char1, char2, char3, 3);
                }
              } else {
                throwUTFEncodingError(index, char1, char2, null, 3);
              }
            } else {
              throw new UTFDataFormatException(
                  "partial 3-byte character at end (char1=" + char1 + ')');
            }
            break;
          default:
            // one byte encoding
            // 0xxxxxxx
            chars[nChars] = (char) char1;
            break;
        }
      }
      pos = limit;
      return new String(chars, 0, nChars);
    } else {
      throw new EOFException();
    }
  }

  /**
   * If the utf encoded data is all ASCII then return
   * a String containing that data. Otherwise return null.
   */
  private String readASCII(int utfLen) {
    final int startIdx = pos;
    int index = pos;
    final int limit = index + utfLen;
    for (; index < limit; index++) {
      if ((bytes[index] & 0xff) >= 128) {
        return null;
      }
    }
    pos = limit;
    @SuppressWarnings("deprecation")
    final String string = new String(bytes, 0, startIdx, utfLen);
    return string;
  }

  /**
   * Behaves like InputStream.read()
   * Returns the next byte as an int in the range [0..255]
   * or -1 if at EOF.
   */
  private int readByteAsInt() {
    if (pos >= nBytes) {
      return -1;
    } else {
      return bytes[pos++] & 0xff;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLine() {
    if (pos >= nBytes) {
      return null;
    }
    // index of the first byte in the line
    int startIdx = pos;
    // index of the last byte in the line
    int lastIdx = -1;
    while (lastIdx == -1) {
      int c = readByteAsInt();
      switch (c) {
        case -1:
          lastIdx = pos;
          break;
        case '\n':
          lastIdx = pos - 1;
          break;
        case '\r':
          lastIdx = pos - 1;
          int c2 = readByteAsInt();
          if (c2 != '\n' && c2 != -1) {
            pos--;
          }
          break;
      }
    }
    @SuppressWarnings("deprecation")
    final String string = new String(bytes, 0, startIdx, lastIdx - startIdx);
    return string;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    bytes = null;
    nBytes = 0;
    pos = 0;
    version = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return version == null ? super.toString()
        : (super.toString() + " (v" + version + ')');
  }

  private void throwUTFEncodingError(int index, int char1, int char2, Integer char3, int enc)
      throws UTFDataFormatException {
    throw new UTFDataFormatException(
        "malformed input for " + enc + "-byte encoding at " + index + " (char1=" + char1 + " char2="
            + char2 + (char3 == null ? ")" : (" char3=" + char3 + ')')));
  }
}
