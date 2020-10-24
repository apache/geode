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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.internal.net.BufferPool.BufferType;

/**
 * Reference counting for a pooled ByteBuffer. Releases buffer back to pool after last reference is
 * dropped.
 */
class ByteBufferReferencing {

  // this variable requires the MakeImmutable annotation but the buffer is empty and
  // not really modifiable
  @MakeImmutable
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final AtomicBoolean isClosed;
  // mutable because in general our ByteBuffer may need to be resized (grown or compacted)
  private ByteBuffer buffer;
  private final BufferType bufferType;
  private final AtomicInteger counter;
  private final BufferPool bufferPool;

  /**
   * Construct with a single reference. You must call {@link #dropReference()} at least one time to
   * return this buffer to the pool.
   */
  ByteBufferReferencing(final ByteBuffer buffer, final BufferType bufferType,
      final BufferPool bufferPool) {
    this.buffer = buffer;
    this.bufferType = bufferType;
    this.bufferPool = bufferPool;
    counter = new AtomicInteger(1);
    isClosed = new AtomicBoolean(false);
  }

  /**
   * The destructor. Called by the resource owner to undo the work of the constructor.
   */
  void close() {
    if (isClosed.compareAndSet(false, true)) {
      dropReference();
    }
  }

  int addReference() {
    return counter.incrementAndGet();
  }

  int dropReference() {
    final int usages = counter.decrementAndGet();
    if (usages == 0) {
      bufferPool.releaseBuffer(bufferType, buffer);
    }
    return usages;
  }

  ByteBuffer getBuffer() throws IOException {
    if (isClosed.get()) {
      throw new IOException("NioSslEngine has been closed");
    } else {
      return buffer;
    }
  }

  ByteBuffer expandWriteBufferIfNeeded(final int newCapacity) {
    return buffer = bufferPool.expandWriteBufferIfNeeded(bufferType, buffer, newCapacity);
  }

  ByteBuffer expandReadBufferIfNeeded(final int newCapacity) {
    return buffer = bufferPool.expandReadBufferIfNeeded(bufferType, buffer, newCapacity);
  }

  @VisibleForTesting
  void setBufferForTestingOnly(final ByteBuffer newBufferForTesting) {
    buffer = newBufferForTesting;
  }
}
