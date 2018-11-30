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

package org.apache.geode.internal.shared;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Intermediate class that extends both an OutputStream and WritableByteChannel.
 *
 * @author swale
 * @since gfxd 1.1
 */
public abstract class OutputStreamChannel extends OutputStream implements
    WritableByteChannel, StreamChannel {

  protected final WritableByteChannel channel;
  private final boolean socketToSameHost;
  private volatile Thread parkedThread;
  protected volatile long bytesWritten;

  protected OutputStreamChannel(WritableByteChannel channel) {
    this.channel = channel;
    this.socketToSameHost = isSocketToSameHost(channel);
  }


  private boolean isSocketToSameHost(Channel channel) {
    try {
      if (channel instanceof SocketChannel) {
        SocketChannel socketChannel = (SocketChannel) channel;
        return isSocketToSameHost(socketChannel.getLocalAddress(),
            socketChannel.getRemoteAddress());
      }
    } catch (IOException ignored) {
    }
    return false;
  }

  private boolean isSocketToSameHost(SocketAddress localSockAddress,
      SocketAddress remoteSockAddress) {
    if ((localSockAddress instanceof InetSocketAddress) &&
        (remoteSockAddress instanceof InetSocketAddress)) {
      InetAddress localAddress = ((InetSocketAddress) localSockAddress)
          .getAddress();
      return localAddress != null && localAddress.equals(
          ((InetSocketAddress) remoteSockAddress).getAddress());
    } else {
      return false;
    }
  }

  public final boolean isSocketToSameHost() {
    return this.socketToSameHost;
  }

  /**
   * Get the underlying {@link WritableByteChannel}.
   */
  public final WritableByteChannel getUnderlyingChannel() {
    return this.channel;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int write(ByteBuffer src) throws IOException;

  /**
   * Writes an <code>int</code> value, which is comprised of four bytes,
   * to the output stream in big-endian format
   * compatible with {@link java.io.DataOutput#writeInt(int)}.
   *
   * @param v the <code>int</code> value to be written.
   * @throws IOException if an I/O error occurs.
   * @see java.io.DataOutput#writeInt(int)
   */
  public abstract void writeInt(int v) throws IOException;

  /**
   * Common base method to write a given ByteBuffer source via an intermediate
   * direct byte buffer owned by the implementation of this class (if required).
   */
  protected final int writeBuffered(final ByteBuffer src,
      final ByteBuffer channelBuffer) throws IOException {
    int srcLen = src.remaining();
    // flush large direct buffers directly into channel
    final boolean flushBuffer = srcLen > (channelBuffer.limit() >>> 1) &&
        src.isDirect();
    int numWritten = 0;
    while (srcLen > 0) {
      final int remaining = channelBuffer.remaining();
      if (srcLen <= remaining) {
        channelBuffer.put(src);
        return (numWritten + srcLen);
      } else {
        // flush directly if there is nothing in the channel buffer
        if (flushBuffer && channelBuffer.position() == 0) {
          return numWritten + writeBufferNoWait(src, this.channel);
        }
        // copy src to buffer and flush
        if (remaining > 0) {
          // lower limit of src temporarily to remaining
          final int srcPos = src.position();
          src.limit(srcPos + remaining);
          try {
            channelBuffer.put(src);
          } finally {
            // restore the limit
            src.limit(srcPos + srcLen);
          }
          srcLen -= remaining;
          numWritten += remaining;
          assert srcLen == src.remaining() : "srcLen=" + srcLen
              + " srcRemaining=" + src.remaining();
        }
        // if we were able to write the full buffer then try writing the
        // remaining from source else return with whatever was written
        if (!flushBufferNonBlockingBase(channelBuffer)) {
          return numWritten;
        } else if (flushBuffer) {
          return numWritten + writeBufferNoWait(src, this.channel);
        }
        // for non-direct buffers use channel buffer for best performance
        // so loop back and try again
      }
    }
    return numWritten;
  }

  protected final boolean flushBufferNonBlockingBase(final ByteBuffer buffer)
      throws IOException {
    buffer.flip();

    final boolean flushed;
    try {
      writeBufferNoWait(buffer, this.channel);
    } finally {
      // if we failed to write the full buffer then compact the remaining bytes
      // to the start so we can start filling it again
      if (buffer.hasRemaining()) {
        buffer.compact();
        flushed = false;
      } else {
        buffer.clear();
        flushed = true;
      }
    }
    return flushed;
  }

  protected int writeBuffer(final ByteBuffer buffer,
      final WritableByteChannel channel) throws IOException {
    long parkedNanos = 0;
    int numTries = 0;
    int numWritten;
    while ((numWritten = channel.write(buffer)) == 0) {
      if (!buffer.hasRemaining()) {
        break;
      }
      // wait for a bit after some retries
      parkedNanos = InputStreamChannel.parkThreadForAsyncOperationIfRequired(
          this, parkedNanos, ++numTries);
    }
    if (numWritten > 0) {
      this.bytesWritten += numWritten;
    }
    return numWritten;
  }

  @Override
  public final Thread getParkedThread() {
    return this.parkedThread;
  }

  @Override
  public final void setParkedThread(Thread thread) {
    this.parkedThread = thread;
  }

  @Override
  public long getParkNanosMax() {
    return InputStreamChannel.PARK_NANOS_MAX;
  }

  protected int writeBufferNoWait(final ByteBuffer buffer,
      final WritableByteChannel channel) throws IOException {
    int numWritten = channel.write(buffer);
    if (numWritten > 0) {
      this.bytesWritten += numWritten;
    }
    return numWritten;
  }

  public final long getBytesWritten() {
    return this.bytesWritten;
  }
}
