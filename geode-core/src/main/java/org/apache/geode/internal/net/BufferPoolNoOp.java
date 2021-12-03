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

import static org.apache.geode.internal.net.BufferPool.BufferType.TRACKED_RECEIVER;
import static org.apache.geode.internal.net.BufferPool.BufferType.TRACKED_SENDER;
import static org.apache.geode.internal.net.BufferPool.BufferType.UNTRACKED;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public class BufferPoolNoOp implements BufferPool {
  public BufferPoolNoOp() {
    System.out.println("BGB: using no-op buffer pool");
  }

  @Override
  public ByteBuffer acquireDirectSenderBuffer(final int size) {
    return acquireDirectBuffer(TRACKED_SENDER, size);
  }

  @Override
  public ByteBuffer acquireDirectReceiveBuffer(final int size) {
    return acquireDirectBuffer(TRACKED_RECEIVER, size);
  }

  @Override
  public ByteBuffer acquireNonDirectSenderBuffer(final int size) {
    return acquireNonDirectBuffer(UNTRACKED, size);
  }

  @Override
  public ByteBuffer acquireNonDirectReceiveBuffer(final int size) {
    return acquireNonDirectBuffer(UNTRACKED, size);
  }

  @Override
  public void releaseSenderBuffer(final ByteBuffer bb) {
    // no-op
  }

  @Override
  public void releaseReceiveBuffer(final ByteBuffer bb) {
    // no-op
  }

  @Override
  public ByteBuffer expandReadBufferIfNeeded(final BufferType type, final ByteBuffer existing,
      final int desiredCapacity) {
    if (existing.capacity() >= desiredCapacity) {
      if (existing.position() > 0) {
        existing.compact();
        existing.flip();
      }
      return existing;
    }
    ByteBuffer newBuffer;
    if (existing.isDirect()) {
      newBuffer = acquireDirectBuffer(type, desiredCapacity);
    } else {
      newBuffer = acquireNonDirectBuffer(type, desiredCapacity);
    }
    newBuffer.clear();
    newBuffer.put(existing);
    newBuffer.flip();
    releaseBuffer(type, existing);
    return newBuffer;
  }

  @Override
  public ByteBuffer expandWriteBufferIfNeeded(final BufferType type, final ByteBuffer existing,
      final int desiredCapacity) {
    if (existing.capacity() >= desiredCapacity) {
      return existing;
    }
    ByteBuffer newBuffer;
    if (existing.isDirect()) {
      newBuffer = acquireDirectBuffer(type, desiredCapacity);
    } else {
      newBuffer = acquireNonDirectBuffer(type, desiredCapacity);
    }
    newBuffer.clear();
    existing.flip();
    newBuffer.put(existing);
    releaseBuffer(type, existing);
    return newBuffer;
  }

  @Override
  public ByteBuffer acquireDirectBuffer(final BufferType _ignored, final int capacity) {
    // type is unimportant for our purposes since we aren't pooling or keeping stats
    return ByteBuffer.allocateDirect(capacity);
  }

  @Override
  public void releaseBuffer(final BufferType type, @NotNull final ByteBuffer buffer) {
    // no-op
  }

  @Override
  public ByteBuffer getPoolableBuffer(final ByteBuffer buffer) {
    // since we aren't pooling the poolable buffer is just the buffer
    return buffer;
  }

  private ByteBuffer acquireNonDirectBuffer(BufferType _ignored, int capacity) {
    // type is unimportant for our purposes since we aren't pooling or keeping stats
    return ByteBuffer.allocate(capacity);
  }

}
