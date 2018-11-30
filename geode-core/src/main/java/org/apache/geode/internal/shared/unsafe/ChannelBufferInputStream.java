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
import java.nio.channels.ReadableByteChannel;

/**
 * Base class for use by buffered abstractions over channel using direct byte
 * buffers. This particular class can be used as a much more efficient
 * replacement for BufferedInputStream.
 * <p>
 * Note that the close() method of this class does not closing the underlying
 * channel.
 *
 * @author swale
 * @since gfxd 1.0
 */
public class ChannelBufferInputStream extends InputStreamChannel {

  protected final ByteBuffer buffer;

  public static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

  public ChannelBufferInputStream(ReadableByteChannel channel) throws IOException {
    this(channel, DEFAULT_BUFFER_SIZE);
  }

  public ChannelBufferInputStream(ReadableByteChannel channel, int bufferSize)
      throws IOException {
    super(channel);
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("invalid bufferSize=" + bufferSize);
    }
    this.buffer = allocateBuffer(bufferSize);
    // flip to force refill on first use
    this.buffer.flip();
  }

  protected ByteBuffer allocateBuffer(int bufferSize) {
    return ByteBuffer.allocate(bufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read() throws IOException {
    if (this.buffer.hasRemaining()) {
      return (this.buffer.get() & 0xff);
    } else if (refillBuffer(this.buffer, 1, null) > 0) {
      return (this.buffer.get() & 0xff);
    } else {
      return -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(byte[] buf, int off, int len) throws IOException {
    if (len == 1) {
      if (this.buffer.hasRemaining()) {
        buf[off] = this.buffer.get();
        return 1;
      } else if (refillBuffer(this.buffer, 1, null) > 0) {
        buf[off] = this.buffer.get();
        return 1;
      } else {
        return -1;
      }
    }

    // first copy anything remaining from buffer
    final int remaining = this.buffer.remaining();
    if (len <= remaining) {
      if (len > 0) {
        this.buffer.get(buf, off, len);
        return len;
      } else {
        return 0;
      }
    }

    // refill buffer once and read whatever available into buf;
    // caller should invoke in a loop if buffer is still not full
    int readBytes = 0;
    if (remaining > 0) {
      this.buffer.get(buf, off, remaining);
      off += remaining;
      len -= remaining;
      readBytes += remaining;
    }
    final int bufBytes = refillBuffer(this.buffer, 1, null);
    if (bufBytes > 0) {
      if (len > bufBytes) {
        len = bufBytes;
      }
      this.buffer.get(buf, off, len);
      return (readBytes + len);
    } else {
      return readBytes > 0 ? readBytes : bufBytes;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int read(ByteBuffer dst) throws IOException {
    return super.readBuffered(dst, this.buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int readInt() throws IOException {
    final ByteBuffer buffer = this.buffer;
    if (buffer.remaining() >= 4) {
      return buffer.getInt();
    } else {
      refillBuffer(buffer, 4, "readInt: premature end of stream");
      return buffer.getInt();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int available() throws IOException {
    return this.buffer.remaining();
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
    this.buffer.clear();
  }
}
