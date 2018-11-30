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
 * Changes for SnappyData distributed computational and data platform.
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
import java.nio.channels.WritableByteChannel;

import org.apache.geode.internal.shared.ChannelBufferOutputStream;
import org.apache.geode.internal.shared.OutputStreamChannel;
import org.apache.geode.pdx.internal.unsafe.UnsafeWrapper;

/**
 * A somewhat more efficient implementation of {@link ChannelBufferOutputStream}
 * using internal unsafe class (~30% in raw single byte write calls).
 * Use {@link UnsafeHolder#newChannelBufferOutputStream} method to create
 * either this or {@link ChannelBufferOutputStream} depending on availability.
 * <p>
 * NOTE: THIS CLASS IS NOT THREAD-SAFE BY DESIGN. IF IT IS USED CONCURRENTLY
 * BY MULTIPLE THREADS THEN BAD THINGS CAN HAPPEN DUE TO UNSAFE MEMORY WRITES.
 * <p>
 * Note that the close() method of this class does not close the underlying
 * channel.
 *
 * @author swale
 * @since gfxd 1.1
 */
public class ChannelBufferUnsafeOutputStream extends OutputStreamChannel {
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

  /**
   * Some minimum buffer size, particularly for longs and encoding UTF strings
   * efficiently. If reducing this, then consider the logic in
   * {@link ChannelBufferUnsafeDataOutputStream#writeUTF(String)} carefully.
   */
  protected static final int MIN_BUFFER_SIZE = 32;

  public ChannelBufferUnsafeOutputStream(WritableByteChannel channel) {
    this(channel, ChannelBufferOutputStream.DEFAULT_BUFFER_SIZE);
  }

  public ChannelBufferUnsafeOutputStream(WritableByteChannel channel,
      int bufferSize) {
    super(channel);
    this.baseAddress = allocateBuffer(bufferSize);
    resetBufferPositions();
  }

  /**
   * Get handle to the underlying ByteBuffer. ONLY TO BE USED BY TESTS.
   */
  public ByteBuffer getInternalBuffer() {
    // set the current position
    this.buffer.position(position());
    return this.buffer;
  }

  protected final void resetBufferPositions() {
    this.addrPosition = this.baseAddress + this.buffer.position();
    this.addrLimit = this.baseAddress + this.buffer.limit();
  }

  protected long allocateBuffer(int bufferSize) {
    // expect minimum bufferSize of 10 bytes
    if (bufferSize < MIN_BUFFER_SIZE) {
      throw new IllegalArgumentException(
          "ChannelBufferUnsafeDataOutputStream: buffersize=" + bufferSize
              + " too small (minimum " + MIN_BUFFER_SIZE + ')');
    }
    // use allocator which will restrict total allocated size
    final ByteBuffer buffer = DirectBufferAllocator.instance().allocateWithFallback(
        bufferSize, "CHANNELOUTPUT");
    // set the order to native explicitly to skip any byte order conversions
    buffer.order(ByteOrder.nativeOrder());
    this.buffer = buffer;

    try {
      return UnsafeHolder.getDirectBufferAddress(buffer);
    } catch (Exception e) {
      releaseBuffer();
      throw new RuntimeException(
          "failed in creating an 'unsafe' buffered channel stream", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void write(int b) throws IOException {
    putByte((byte) (b & 0xff));
  }

  protected final void write_(byte[] b, int off, int len) throws IOException {
    if (len == 1) {
      putByte(b[off]);
      return;
    }

    while (len > 0) {
      final long addrPos = this.addrPosition;
      final int remaining = (int) (this.addrLimit - addrPos);
      if (len <= remaining) {
        unsafe.copyMemory(b, unsafe.arrayBaseOffset(byte[].class) + off,
            null, addrPos, len);
        this.addrPosition += len;
        return;
      } else {
        // copy b to buffer and flush
        if (remaining > 0) {
          unsafe.copyMemory(b, unsafe.arrayBaseOffset(byte[].class) + off,
              null, addrPos, remaining);
          this.addrPosition += remaining;
          len -= remaining;
          off += remaining;
        }
        flushBufferBlocking(this.buffer);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void write(byte[] b) throws IOException {
    write_(b, 0, b.length);
  }

  protected final void putByte(byte b) throws IOException {
    if (this.addrPosition >= this.addrLimit) {
      flushBufferBlocking(this.buffer);
    }
    unsafe.putByte(null, this.addrPosition++, b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void write(byte[] b,
      int off, int len) throws IOException {
    UnsafeHolder.checkBounds(b.length, off, len);
    write_(b, off, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int write(ByteBuffer src) throws IOException {
    // We will just use our ByteBuffer for the write. It might be possible
    // to get slight performance advantage in using unsafe instead, but
    // copying from source ByteBuffer will not be efficient without
    // reflection to get src's native address in case it is a direct
    // byte buffer. Avoiding the complication since the benefit will be
    // very small in any case (and reflection cost may well offset that).

    // adjust this buffer position first
    this.buffer.position((int) (this.addrPosition - this.baseAddress));
    // now we are actually set to just call base class method
    try {
      return super.writeBuffered(src, this.buffer);
    } finally {
      // finally reset the raw positions from buffer
      resetBufferPositions();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeInt(int v) throws IOException {
    long addrPos = this.addrPosition;
    if ((this.addrLimit - addrPos) < 4) {
      flushBufferBlocking(this.buffer);
      addrPos = this.addrPosition;
    }
    this.addrPosition = putInt(addrPos, v);
  }

  public final int position() {
    return (int) (this.addrPosition - this.baseAddress);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    final ByteBuffer buffer;
    if (this.addrPosition > this.baseAddress &&
        (buffer = this.buffer) != null) {
      flushBufferBlocking(buffer);
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
  public void close() throws IOException {
    flush();
    this.addrPosition = this.addrLimit = 0;
    releaseBuffer();
  }

  protected final void releaseBuffer() {
    final ByteBuffer buffer = this.buffer;
    if (buffer != null) {
      this.buffer = null;
      DirectBufferAllocator.instance().release(buffer);
    }
  }

  /**
   * Close the underlying channel in addition to flushing/clearing the buffer.
   */
  public void closeChannel() throws IOException {
    flush();
    this.addrPosition = this.addrLimit = 0;
    this.channel.close();
    releaseBuffer();
  }

  protected void flushBufferBlocking(final ByteBuffer buffer)
      throws IOException {
    buffer.position(position());
    buffer.flip();
    try {
      do {
        writeBuffer(buffer, this.channel);
      } while (buffer.hasRemaining());
    } finally {
      if (buffer.hasRemaining()) {
        buffer.compact();
      } else {
        buffer.clear();
      }
      resetBufferPositions();
    }
  }

  /** Write an integer in big-endian format on given off-heap address. */
  protected static long putInt(long addrPos, final int v) {
    if (UnsafeHolder.littleEndian) {
      unsafe.putInt(null, addrPos, Integer.reverseBytes(v));
    } else {
      unsafe.putInt(null, addrPos, v);
    }
    return addrPos + 4;
  }
}
