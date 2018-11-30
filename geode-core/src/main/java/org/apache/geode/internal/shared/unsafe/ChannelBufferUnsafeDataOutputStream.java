/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.geode.internal.shared.unsafe;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

import org.apache.geode.pdx.internal.unsafe.UnsafeWrapper;


/**
 * A buffered DataOutput abstraction over channel using direct byte buffers, and
 * using internal Unsafe class for best performance. Users must check for
 * {@link UnsafeHolder#hasUnsafe()} before trying to use this class.
 * <p>
 * The implementation is not thread-safe by design. This particular class can be
 * used as an efficient, buffered DataOutput implementation for file channels,
 * socket channels and other similar.
 *
 * @author swale
 * @since gfxd 1.0
 */
public class ChannelBufferUnsafeDataOutputStream extends
    ChannelBufferUnsafeOutputStream implements DataOutput {
  private static UnsafeWrapper unsafe = new UnsafeWrapper();

  public ChannelBufferUnsafeDataOutputStream(WritableByteChannel channel) {
    super(channel);
  }

  public ChannelBufferUnsafeDataOutputStream(WritableByteChannel channel,
      int bufferSize) {
    super(channel, bufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeBoolean(boolean v) throws IOException {
    putByte(v ? (byte) 1 : 0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeByte(int v) throws IOException {
    putByte((byte) (v & 0xff));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeShort(int v) throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) < 2) {
      flushBufferBlocking(this.buffer);
      addrPos = this.addrPosition;
    }
    this.addrPosition = putShort(addrPos, v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeChar(int v) throws IOException {
    writeShort(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeLong(long v) throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) < 8) {
      flushBufferBlocking(this.buffer);
      addrPos = this.addrPosition;
    }
    this.addrPosition = putLong(addrPos, v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeBytes(String s) throws IOException {
    if (s.length() > 0) {
      write(s.getBytes(StandardCharsets.US_ASCII));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeChars(String s) throws IOException {
    int off = 0;
    int len = s.length();
    while (len > 0) {
      long addrPos = this.addrPosition;
      final int remaining = (int) (this.addrLimit - addrPos);
      if ((len << 1) <= remaining) {
        final int end = (off + len);
        while (off < end) {
          addrPos = putShort(addrPos, s.charAt(off++));
        }
        this.addrPosition = addrPos;
        return;
      } else {
        final int remchars = (remaining >>> 1);
        final int end = (off + remchars);
        while (off < end) {
          addrPos = putShort(addrPos, s.charAt(off++));
        }
        this.addrPosition = addrPos;
        flushBufferBlocking(this.buffer);
        len -= remchars;
      }
    }
  }

  private int getUTFLength(final String str, final int strLen) {
    int utfLen = strLen;
    for (int i = 0; i < strLen; i++) {
      final char c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        // 1 byte for character
        continue;
      } else if (c > 0x07FF) {
        utfLen += 2; // 3 bytes for character
      } else {
        utfLen++; // 2 bytes for character
      }
    }
    return utfLen;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeUTF(String str) throws IOException {
    int strLen = str.length();
    if (strLen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + strLen);
    }

    // first check the optimistic case where worst case of 2 for length + 3 for
    // each char fits into remaining space in buffer
    long addrPos = this.addrPosition;
    long remaining = this.addrLimit - addrPos;
    if (remaining >= ((strLen * 3) + 2)) {
      // write the UTF string skipping the length, then write length at the last
      addrPos += 2;
      final long finalAddrPos = writeUTFSegmentNoOverflow(str, 0, strLen,
          -1, null, addrPos);
      long utflen = (finalAddrPos - addrPos);
      if (utflen <= 65535) {
        putShort(addrPos - 2, (int) utflen);
        this.addrPosition = finalAddrPos;
      } else {
        // act as if we wrote nothing to this buffer (no change to addrPosition)
        throw new UTFDataFormatException("encoded string too long: " + utflen
            + " bytes");
      }
      return;
    }


    // otherwise first calculate the UTF encoded length, write it in buffer
    // (which may need to be flushed at any point), then break string into worst
    // case segments for writing to buffer and flushing if end of buffer reached
    int utfLen = getUTFLength(str, strLen);
    if (utfLen > 65535) {
      throw new UTFDataFormatException("encoded string too long: " + utfLen
          + " bytes");
    }
    // write the length first
    if (remaining > 2) {
      addrPos = putShort(addrPos, utfLen);
      remaining -= 2;
    } else {
      flushBufferBlocking(this.buffer);
      addrPos = putShort(this.addrPosition, utfLen);
      remaining = this.addrLimit - addrPos;
    }

    // next break string into segments assuming worst case of 3 bytes per char,
    // flushing buffer as required after each segment write
    int offset = 0;
    while (strLen > 0) {
      int writeLen = Math.min(strLen, (int) (remaining / 3));
      if (writeLen >= 3) {
        // write the UTF segment and update the number of remaining characters,
        // offset, remaining buffer size etc
        long newAddrPos = writeUTFSegmentNoOverflow(str, offset, writeLen,
            -1, null, addrPos);
        strLen -= writeLen;
        offset += writeLen;
        remaining -= (newAddrPos - addrPos);
        addrPos = newAddrPos;
      } else {
        // if we have too few to write then better to flush the buffer and then
        // try (bufferSize is at least 10 as ensured in constructors)
        this.addrPosition = addrPos;
        flushBufferBlocking(this.buffer);
        remaining = this.addrLimit - (addrPos = this.addrPosition);
      }
    }
    this.addrPosition = addrPos;
  }

  public static long writeUTFSegmentNoOverflow(String str, int offset,
      int length, final int utfLen, final Object target, long addrPos) {
    final int end = (offset + length);
    // fast path for ASCII strings
    if (length == utfLen) {
      while (offset < end) {
        final char c = str.charAt(offset++);
        unsafe.putByte(target, addrPos++, (byte) c);
      }
      return addrPos;
    }
    while (offset < end) {
      final char c = str.charAt(offset++);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        unsafe.putByte(target, addrPos++, (byte) c);
      } else if (c > 0x07FF) {
        unsafe.putByte(target, addrPos++, (byte) (0xE0 | ((c >> 12) & 0x0F)));
        unsafe.putByte(target, addrPos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
        unsafe.putByte(target, addrPos++, (byte) (0x80 | (c & 0x3F)));
      } else {
        unsafe.putByte(target, addrPos++, (byte) (0xC0 | ((c >> 6) & 0x1F)));
        unsafe.putByte(target, addrPos++, (byte) (0x80 | (c & 0x3F)));
      }
    }
    return addrPos;
  }

  /** Write a short in big-endian format on given off-heap address. */
  protected static long putShort(long addrPos, final int v) {
    if (UnsafeHolder.littleEndian) {
      unsafe.putShort(null, addrPos, Short.reverseBytes((short) v));
    } else {
      unsafe.putShort(null, addrPos, (short) v);
    }
    return addrPos + 2;
  }

  /** Write a long in big-endian format on given off-heap address. */
  protected static long putLong(long addrPos, final long v) {
    if (UnsafeHolder.littleEndian) {
      unsafe.putLong(null, addrPos, Long.reverseBytes(v));
    } else {
      unsafe.putLong(null, addrPos, v);
    }
    return addrPos + 8;
  }
}
