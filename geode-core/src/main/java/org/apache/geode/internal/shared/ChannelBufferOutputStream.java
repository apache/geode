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

package org.apache.geode.internal.shared;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * OutputStream for use by buffered write abstractions over channel using direct
 * byte buffers. The implementation is not thread-safe by design. The class
 * itself can be used as a non-synchronized efficient replacement of
 * BufferedOutputStream.
 * <p>
 * Note that the close() method of this class does not closing the underlying
 * channel.
 *
 * @author swale
 * @since gfxd 1.1
 */
public class ChannelBufferOutputStream extends OutputStreamChannel {

  protected final ByteBuffer buffer;

  public static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

  public ChannelBufferOutputStream(WritableByteChannel channel)
      throws IOException {
    this(channel, DEFAULT_BUFFER_SIZE);
  }

  public ChannelBufferOutputStream(WritableByteChannel channel, int bufferSize)
      throws IOException {
    super(channel);
    if (bufferSize < 32) {
      throw new IllegalArgumentException("buffer size " + bufferSize +
          " should be at least 32");
    }
    this.buffer = allocateBuffer(bufferSize).order(ByteOrder.BIG_ENDIAN);
  }

  protected ByteBuffer allocateBuffer(int bufferSize) {
    return ByteBuffer.allocate(bufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void write(int b) throws IOException {
    if (!this.buffer.hasRemaining()) {
      flushBufferBlocking(this.buffer);
    }
    this.buffer.put((byte) (b & 0xff));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void write(byte[] b,
      int off, int len) throws IOException {
    if (len == 1) {
      write(b[off]);
      return;
    }

    while (len > 0) {
      int remaining = this.buffer.remaining();
      if (len <= remaining) {
        this.buffer.put(b, off, len);
        return;
      } else {
        // copy b to buffer and flush
        if (remaining > 0) {
          this.buffer.put(b, off, remaining);
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
  public final int write(ByteBuffer src) throws IOException {
    return super.writeBuffered(src, this.buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void writeInt(int v) throws IOException {
    if (this.buffer.remaining() < 4) {
      flushBufferBlocking(this.buffer);
    }
    // ByteBuffer will always be big-endian
    this.buffer.putInt(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    if (this.buffer.position() > 0) {
      flushBufferBlocking(this.buffer);
    }
  }

  /**
   * {@inheritDoc}
   */
  public final boolean isOpen() {
    return this.channel.isOpen();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    flush();
  }

  protected void flushBufferBlocking(final ByteBuffer buffer)
      throws IOException {
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
    }
  }
}
