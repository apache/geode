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

import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public class BufferDebuggingCircularBuffer {
  private final ByteBuffer circularBuffer;

  /*
   * These are indices on writableCircularBuffer. They behave a little differently from
   * their counterparts (position, limit) in a readable ByteBuffer. See details below.
   */

  // offset of next readable value of writableCircularBuffer
  private int readPosition = 0;
  // (one more than offset of last readable value) % capacity (of writableCircularBuffer)
  private int readLimit = 0;

  /*
   * readPosition == readLimit in two situations: empty buffer and full buffer.
   * This flag differentiates those situations.
   */
  private boolean isEmpty = true;

  // public BufferDebuggingCircularBuffer() {
  // this(DEFAULT_SOCKET_BUFFER_SIZE);
  // }

  public BufferDebuggingCircularBuffer(final int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be greater than zero");
    }
    circularBuffer = ByteBuffer.allocateDirect(capacity);
  }

  // readableSourceBuffer is readable on entry and exit
  // readableSourceBuffer is mutated (it is read)
  public BufferDebuggingCircularBuffer put(ByteBuffer wholeReadableSourceBuffer) {

    // fastForwardOptimization(wholeReadableSourceBuffer, writableCircularBuffer);

    while (wholeReadableSourceBuffer.remaining() > 0) {
      final int capacity = circularBuffer.capacity() - readLimit;
      assert capacity > 0;
      final int addSize =
          Math.min(wholeReadableSourceBuffer.remaining(), capacity);
      assert addSize > 0;

      // transfer data
      final ByteBuffer src = wholeReadableSourceBuffer.slice();
      src.limit(addSize);
      circularBuffer.put(src);
      advancePosition(wholeReadableSourceBuffer, addSize);

      // restore invariants

      final boolean wasFull = isFull();
      isEmpty = false;

      readLimit += addSize;
      assert readLimit <= circularBuffer.capacity();

      if (readLimit == circularBuffer.capacity()) {
        readLimit = 0; // wrap!
      }
      if (wasFull) {
        // writing to a full buffer drops oldest data
        readPosition = readLimit;
      }
      if (circularBuffer.remaining() == 0) {
        circularBuffer.position(0); // wrap!
      }
    }
    return this;
  }

  public BufferDebuggingCircularBuffer get(final byte[] destination, final int offset,
      final int length) {
    if (isEmpty) {
      // no-op
    } else if (readPosition < readLimit) {
      // [readPosition,readLimit)
      final ByteBuffer src = circularBuffer.duplicate();
      src.position(readPosition).limit(readLimit);
      src.get(destination, offset, length);
    } else { // readPosition >= readLimit) {
      // [readPosition, N), [0,readLimit)
      final int size1 = circularBuffer.capacity() - readPosition;
      final ByteBuffer src1 = circularBuffer.duplicate();
      src1.position(readPosition).limit(src1.capacity());
      src1.get(destination, offset, size1);
      if (length > size1) {
        final int size2 = length - size1;
        final ByteBuffer src2 = circularBuffer.duplicate();
        src2.position(0).limit(readLimit);
        src2.get(destination, offset + size1, size2);
      }
    }
    return this;
  }

  private boolean isFull() {
    return !isEmpty && readPosition == readLimit;
  }

  /*
   * if source buffer is not smaller than destination buffer then we can just transfer
   * one (destination buffer's worth) of data from the end of the source (ignoring the
   * preceding source data)
   *
   * mutates both wholeReadableSourceBuffer and writableCircularBuffer
   */
  private void fastForwardOptimization(final ByteBuffer wholeReadableSourceBuffer,
      final ByteBuffer writableCircularBuffer) {
    final int fastForwardOffset =
        fastForwardOffset(wholeReadableSourceBuffer.remaining(), writableCircularBuffer.capacity());
    if (fastForwardOffset > 0) {
      // TODO: make this method update readPosition and readLimit too
      advancePosition(wholeReadableSourceBuffer, fastForwardOffset);
      final int newDestinationPosition =
          advancePositionWithWrap(writableCircularBuffer, fastForwardOffset);
      readPosition = readLimit = newDestinationPosition;
      isEmpty = false;
    }
  }

  @NotNull
  private static int advancePositionWithWrap(final ByteBuffer buffer,
      final int offset) {
    final int newPosition = (buffer.position() + offset)
        % buffer.capacity();
    buffer.position(newPosition);
    return newPosition;
  }

  @NotNull
  private static Buffer advancePosition(final ByteBuffer buffer,
      final int offset) {
    return buffer.position(buffer.position() + offset);
  }

  public static int fastForwardOffset(final int remainingReadable, final int capacityWritable) {
    if (capacityWritable == 0) {
      return 0;
    }
    final int skip = remainingReadable - capacityWritable;
    if (skip < 0) {
      return 0;
    }
    return skip;
  }

}
