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
/*
 * Changes for SnappyData distributed computational and data unsafe.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import org.apache.geode.internal.shared.ChannelBufferInputStream;
import org.apache.geode.internal.shared.InputStreamChannel;
import org.apache.geode.pdx.internal.unsafe.UnsafeWrapper;

/**
 * A more efficient implementation of {@link ChannelBufferInputStream}
 * using internal unsafe class (~30% in raw read calls).
 * Use {@link UnsafeHolder#newChannelBufferInputStream} method to
 * create either this or {@link ChannelBufferInputStream} depending on
 * availability.
 * <p>
 * Note that the close() method of this class does not closing the underlying
 * channel.
 *
 * @author swale
 * @since gfxd 1.1
 */
public class ChannelBufferUnsafeInputStream extends InputStreamChannel {
  private static UnsafeWrapper unsafe = new UnsafeWrapper();

  protected ByteBuffer buffer;
  protected final long baseAddress;
  /**
   * Actual buffer position (+baseAddress) accounting is done by this. Buffer
   * position is adjusted during refill and other places where required using
   * this.
   */
  protected long addrPosition;
  protected long addrLimit;

  public ChannelBufferUnsafeInputStream(ReadableByteChannel channel) {
    this(channel, ChannelBufferInputStream.DEFAULT_BUFFER_SIZE);
  }

  public ChannelBufferUnsafeInputStream(ReadableByteChannel channel,
      int bufferSize) {
    super(channel);
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("invalid bufferSize=" + bufferSize);
    }
    this.buffer = allocateBuffer(bufferSize);
    // force refill on first use
    this.buffer.position(bufferSize);

    try {
      this.baseAddress = UnsafeHolder.getDirectBufferAddress(this.buffer);
      resetBufferPositions();
    } catch (Exception e) {
      throw new RuntimeException(
          "failed in creating an 'unsafe' buffered channel stream", e);
    }
  }

  protected final void resetBufferPositions() {
    this.addrPosition = this.baseAddress + this.buffer.position();
    this.addrLimit = this.baseAddress + this.buffer.limit();
  }

  protected ByteBuffer allocateBuffer(int bufferSize) {
    // use allocator which will restrict total allocated size
    ByteBuffer buffer = DirectBufferAllocator.instance().allocateWithFallback(
        bufferSize, "CHANNELINPUT");
    // set the order to native explicitly to skip any byte order conversions
    buffer.order(ByteOrder.nativeOrder());
    return buffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read() throws IOException {
    if (this.addrPosition >= this.addrLimit) {
      if (refillBuffer(this.buffer, 1, null) <= 0) {
        return -1;
      }
    }
    return unsafe.getByte(null, this.addrPosition++) & 0xff;
  }

  private int read_(byte[] buf, int off, int len) throws IOException {
    if (len == 1) {
      final int b = read();
      if (b != -1) {
        buf[off] = (byte) b;
        return 1;
      } else {
        return -1;
      }
    }

    // first copy anything remaining from buffer
    final int remaining = (int) (this.addrLimit - this.addrPosition);
    if (len <= remaining) {
      if (len > 0) {
        unsafe.copyMemory(null, this.addrPosition, buf,
            unsafe.arrayBaseOffset(byte[].class) + off, len);
        this.addrPosition += len;
        return len;
      } else {
        return 0;
      }
    }

    // refill buffer once and read whatever available into buf;
    // caller should invoke in a loop if buffer is still not full
    if (remaining > 0) {
      unsafe.copyMemory(null, this.addrPosition, buf,
          unsafe.arrayBaseOffset(byte[].class) + off, remaining);
      this.addrPosition += remaining;
      return remaining;
    }
    final int bufBytes = refillBuffer(this.buffer, 1, null);
    if (bufBytes > 0) {
      if (len > bufBytes) {
        len = bufBytes;
      }
      unsafe.copyMemory(null, this.addrPosition, buf,
          unsafe.arrayBaseOffset(byte[].class) + off, len);
      this.addrPosition += len;
      return len;
    } else {
      return bufBytes;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(byte[] buf) throws IOException {
    return read_(buf, 0, buf.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(byte[] buf,
      int off, int len) throws IOException {
    UnsafeHolder.checkBounds(buf.length, off, len);
    return read_(buf, off, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(ByteBuffer dst) throws IOException {
    // We will just use our ByteBuffer for the read. It might be possible
    // to get slight performance advantage in using unsafe instead, but
    // copying to ByteBuffer will not be efficient without reflection
    // to get dst's native address in case it is a direct byte buffer.
    // Avoiding the complication since the benefit will be very small
    // in any case (and reflection cost may well offset that).
    // We can use unsafe for a small perf benefit for heap byte buffers.

    // adjust this buffer position first
    this.buffer.position((int) (this.addrPosition - this.baseAddress));
    try {
      // now we are set to just call base class method
      return super.readBuffered(dst, this.buffer);
    } finally {
      // finally reset the raw positions from buffer
      resetBufferPositions();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int readInt() throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) < 4) {
      refillBuffer(this.buffer, 4, "readInt: premature end of stream");
      addrPos = this.addrPosition;
    }
    this.addrPosition += 4;
    if (UnsafeHolder.littleEndian) {
      return Integer.reverseBytes(unsafe.getInt(null, addrPos));
    } else {
      return unsafe.getInt(null, addrPos);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int available() {
    return (int) (this.addrLimit - this.addrPosition);
  }

  @Override
  protected int refillBuffer(final ByteBuffer channelBuffer,
      final int tryReadBytes, final String eofMessage) throws IOException {
    // adjust this buffer position first
    channelBuffer.position((int) (this.addrPosition - this.baseAddress));
    try {
      return super.refillBuffer(channelBuffer, tryReadBytes, eofMessage);
    } finally {
      // adjust back position and limit
      resetBufferPositions();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean isOpen() {
    return this.channel.isOpen();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    final ByteBuffer buffer = this.buffer;
    if (buffer != null) {
      this.addrPosition = this.addrLimit = 0;
      this.buffer = null;
      DirectBufferAllocator.instance().release(buffer);
    }
  }
}
