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


import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;

/**
 * A pass-through implementation of NioFilter. Use this if you don't need
 * secure communications.
 */
public class NioPlainEngine implements NioFilter {
  private static final Logger logger = LogService.getLogger();

  int lastReadPosition;
  int lastProcessedPosition;


  public NioPlainEngine() {}

  @Override
  public ByteBuffer wrap(ByteBuffer buffer) {
    return buffer;
  }

  @Override
  public ByteBuffer unwrap(ByteBuffer wrappedBuffer) {
    wrappedBuffer.position(wrappedBuffer.limit());
    return wrappedBuffer;
  }

  @Override
  public ByteBuffer ensureWrappedCapacity(int amount, ByteBuffer wrappedBuffer,
      Buffers.BufferType bufferType, DMStats stats) {
    ByteBuffer buffer = wrappedBuffer;

    if (buffer.capacity() > amount) {
      // we already have a buffer that's big enough
      if (buffer.capacity() - lastProcessedPosition < amount) {
        buffer.limit(lastReadPosition);
        buffer.position(lastProcessedPosition);
        buffer.compact();
        lastReadPosition = buffer.position();
        lastProcessedPosition = 0;
      }
    } else {
      ByteBuffer oldBuffer = buffer;
      oldBuffer.limit(lastReadPosition);
      oldBuffer.position(lastProcessedPosition);
      buffer = Buffers.acquireBuffer(bufferType, amount, stats);
      buffer.clear();
      buffer.put(oldBuffer);
      lastReadPosition = buffer.position();
      lastProcessedPosition = 0;
    }
    return buffer;
  }

  @Override
  public ByteBuffer readAtLeast(SocketChannel channel, int bytes, ByteBuffer wrappedBuffer,
      DMStats stats) throws IOException {
    ByteBuffer buffer = wrappedBuffer;

    Assert.assertTrue(buffer.capacity() - lastProcessedPosition >= bytes);

    // read into the buffer starting at the end of valid data
    buffer.limit(buffer.capacity());
    buffer.position(lastReadPosition);

    while (buffer.position() < (lastProcessedPosition + bytes)) {
      int amountRead = channel.read(buffer);
      if (amountRead < 0) {
        throw new EOFException();
      }
    }

    // keep track of how much of the buffer contains valid data with lastReadPosition
    lastReadPosition = buffer.position();

    // set up the buffer for reading and keep track of how much has been consumed with
    // lastProcessedPosition
    buffer.limit(lastProcessedPosition + bytes);
    buffer.position(lastProcessedPosition);
    lastProcessedPosition += bytes;

    return buffer;
  }

  public void doneReading(ByteBuffer unwrappedBuffer) {
    if (unwrappedBuffer.position() != 0) {
      unwrappedBuffer.compact();
    } else {
      unwrappedBuffer.position(unwrappedBuffer.limit());
      unwrappedBuffer.limit(unwrappedBuffer.capacity());
    }
  }

  @Override
  public ByteBuffer getUnwrappedBuffer(ByteBuffer wrappedBuffer) {
    return wrappedBuffer;
  }

}
