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
package org.apache.geode.internal.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Prior to transmitting a buffer or processing a received buffer
 * a NioFilter should be called to wrap (transmit) or unwrap (received)
 * the buffer in case SSL is being used.<br>
 * Implementations of this class may not be thread-safe in regard to
 * the buffers their methods return. These may be internal state that,
 * if used concurrently by multiple threads could cause corruption.
 * Appropriate external synchronization must be used in order to provide
 * thread-safety. Do this by invoking getSynchObject() and synchronizing on
 * the returned object while using the buffer.
 */
public interface NioFilter {

  /**
   * wrap bytes for transmission to another process
   */
  ByteBuffer wrap(ByteBuffer buffer) throws IOException;

  /**
   * unwrap bytes received from another process. The unwrapped
   * buffer should be flipped before reading. When done reading invoke
   * doneReading() to reset for future read ops
   */
  ByteBuffer unwrap(ByteBuffer wrappedBuffer) throws IOException;

  /**
   * ensure that the wrapped buffer has enough room to read the given amount of data.
   * This must be invoked before readAtLeast. A new buffer may be returned by this method.
   */
  ByteBuffer ensureWrappedCapacity(int amount, ByteBuffer wrappedBuffer,
      BufferPool.BufferType bufferType);

  /**
   * read at least the indicated amount of bytes from the given
   * socket. The buffer position will be ready for reading
   * the data when this method returns. Note: you must invoke ensureWrappedCapacity
   * with the given amount prior to each invocation of this method.
   * <br>
   * wrappedBuffer = filter.ensureWrappedCapacity(amount, wrappedBuffer, etc.);<br>
   * unwrappedBuffer = filter.readAtLeast(channel, amount, wrappedBuffer, etc.)
   */
  ByteBuffer readAtLeast(SocketChannel channel, int amount, ByteBuffer wrappedBuffer)
      throws IOException;

  /**
   * When done reading a direct ack message invoke this method
   */
  default void doneReadingDirectAck(ByteBuffer unwrappedBuffer) {
    doneReading(unwrappedBuffer);
  }

  /**
   * You must invoke this when done reading from the unwrapped buffer
   */
  default void doneReading(ByteBuffer unwrappedBuffer) {
    if (unwrappedBuffer.position() != 0) {
      unwrappedBuffer.compact();
    } else {
      unwrappedBuffer.position(unwrappedBuffer.limit());
      unwrappedBuffer.limit(unwrappedBuffer.capacity());
    }
  }

  default boolean isClosed() {
    return false;
  }

  /**
   * invoke this method when you are done using the NioFilter
   *
   */
  default void close(SocketChannel socketChannel) {
    // nothing by default
  }

  /**
   * returns the unwrapped byte buffer associated with the given wrapped buffer.
   */
  ByteBuffer getUnwrappedBuffer(ByteBuffer wrappedBuffer);

  /**
   * returns an object to be used in synchronizing on the use of buffers returned by
   * a NioFilter.
   */
  default Object getSynchObject() {
    return this;
  }
}
