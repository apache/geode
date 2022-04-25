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

import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.net.BufferPool.PooledByteBuffer;

/**
 * A pass-through implementation of NioFilter. Use this if you don't need
 * secure communications.
 */
public class NioPlainEngine implements NioFilter {

  // this variable requires the MakeImmutable annotation but the buffer is empty and
  // not really modifiable
  @MakeImmutable
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final BufferPool bufferPool;

  int lastReadPosition;
  int lastProcessedPosition;


  public NioPlainEngine(BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  @Override
  public ByteBufferSharing wrap(ByteBuffer buffer) {
    return shareBuffer(buffer);
  }

  @Override
  public ByteBufferSharing unwrap(ByteBuffer wrappedBuffer) {
    wrappedBuffer.position(wrappedBuffer.limit());
    return shareBuffer(wrappedBuffer);
  }

  @Override
  public PooledByteBuffer ensureWrappedCapacity(int amount, PooledByteBuffer pooledWrappedBuffer,
      BufferPool.BufferType bufferType) {

    if (pooledWrappedBuffer == null) {
      pooledWrappedBuffer = bufferPool.acquireDirectBuffer(bufferType, amount);
      pooledWrappedBuffer.getByteBuffer().clear();
      lastProcessedPosition = 0;
      lastReadPosition = 0;
    } else if (pooledWrappedBuffer.getByteBuffer().capacity() > amount) {
      // we already have a buffer that's big enough
      ByteBuffer buffer = pooledWrappedBuffer.getByteBuffer();
      if (buffer.capacity() - lastProcessedPosition < amount) {
        buffer.limit(lastReadPosition);
        buffer.position(lastProcessedPosition);
        buffer.compact();
        lastReadPosition = buffer.position();
        lastProcessedPosition = 0;
      }
    } else {
      ByteBuffer oldBuffer = pooledWrappedBuffer.getByteBuffer();
      oldBuffer.limit(lastReadPosition);
      oldBuffer.position(lastProcessedPosition);
      PooledByteBuffer newPooledBuffer = bufferPool.acquireDirectBuffer(bufferType, amount);
      ByteBuffer buffer = newPooledBuffer.getByteBuffer();
      buffer.clear();
      buffer.put(oldBuffer);
      bufferPool.releaseBuffer(bufferType, pooledWrappedBuffer);
      lastReadPosition = buffer.position();
      lastProcessedPosition = 0;
      pooledWrappedBuffer = newPooledBuffer;
    }
    return pooledWrappedBuffer;
  }

  @Override
  public ByteBufferSharing readAtLeast(SocketChannel channel, int bytes, ByteBuffer buffer)
      throws IOException {
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

    return shareBuffer(buffer);
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
  public ByteBufferSharing getUnwrappedBuffer() {
    return shareBuffer(EMPTY_BUFFER);
  }

  private ByteBufferSharingNoOp shareBuffer(final ByteBuffer wrappedBuffer) {
    return new ByteBufferSharingNoOp(wrappedBuffer);
  }

}
