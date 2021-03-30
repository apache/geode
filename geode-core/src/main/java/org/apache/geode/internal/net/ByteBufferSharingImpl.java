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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.net.BufferPool.BufferType;

/**
 * An {@link AutoCloseable} meant to be acquired in a try-with-resources statement. The resource (a
 * {@link ByteBuffer}) is available (for reading and modification) in the scope of the
 * try-with-resources.
 */
public class ByteBufferSharingImpl implements ByteBufferSharing {

  static class OpenAttemptTimedOut extends Exception {
  }

  private final Lock lock;
  private final AtomicBoolean isDestructed;
  // mutable because in general our ByteBuffer may need to be resized (grown or compacted)
  private volatile ByteBuffer buffer;
  private final BufferType bufferType;
  private final AtomicInteger counter;
  private final BufferPool bufferPool;

  /**
   * This constructor is for use only by the owner of the shared resource (a {@link ByteBuffer}).
   *
   * A resource owner must invoke {@link #open()} once for each reference that escapes (is passed
   * to an external object or is returned to an external caller.)
   *
   * This constructor acquires no lock. The reference count will be 1 after this constructor
   * completes.
   */
  public ByteBufferSharingImpl(final ByteBuffer buffer, final BufferType bufferType,
      final BufferPool bufferPool) {
    this.buffer = buffer;
    this.bufferType = bufferType;
    this.bufferPool = bufferPool;
    lock = new ReentrantLock();
    counter = new AtomicInteger(1);
    isDestructed = new AtomicBoolean(false);
  }

  /**
   * The destructor. Called by the resource owner to undo the work of the constructor.
   */
  public void destruct() {
    if (isDestructed.compareAndSet(false, true)) {
      dropReference();
    }
  }

  /**
   * This method is for use only by the owner of the shared resource. It's used for handing out
   * references to the shared resource. So it does reference counting and also acquires a lock.
   *
   * Resource owners call this method as the last thing before returning a reference to the caller.
   * That caller binds that reference to a variable in a try-with-resources statement and relies on
   * the AutoCloseable protocol to invoke {@link #close()} on the object at the end of the block.
   */
  public ByteBufferSharing open() throws IOException {
    lock.lock();
    addReferenceAfterLock();
    return this;
  }

  /**
   * This variant throws {@link OpenAttemptTimedOut} if it can't acquire the lock in time.
   */
  public ByteBufferSharing open(final long time, final TimeUnit unit)
      throws OpenAttemptTimedOut, IOException {
    try {
      if (!lock.tryLock(time, unit)) {
        throw new OpenAttemptTimedOut();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OpenAttemptTimedOut();
    }
    addReferenceAfterLock();
    return this;
  }

  @Override
  public ByteBuffer getBuffer() throws IOException {
    if (isDestructed.get()) {
      throwClosed();
    }
    return buffer;
  }

  @Override
  public ByteBuffer expandWriteBufferIfNeeded(final int newCapacity) throws IOException {
    return buffer = bufferPool.expandWriteBufferIfNeeded(bufferType, getBuffer(), newCapacity);
  }

  @Override
  public ByteBuffer expandReadBufferIfNeeded(final int newCapacity) throws IOException {
    return buffer = bufferPool.expandReadBufferIfNeeded(bufferType, getBuffer(), newCapacity);
  }

  @Override
  public void close() {
    /*
     * We are counting on our ReentrantLock throwing an exception if the current thread
     * does not hold the lock. In that case dropReference() will not be called. This
     * prevents ill-behaved clients (clients that call close() too many times) from
     * corrupting our reference count.
     */
    lock.unlock();
    dropReference();
  }

  private int addReference() throws IOException {
    int usages;
    do {
      usages = counter.get();
      if (usages == 0 || isDestructed.get()) {
        throwClosed();
      }
    } while (!counter.compareAndSet(usages, usages + 1));
    return usages + 1;
  }

  private int dropReference() {
    final int usages = counter.decrementAndGet();
    if (usages == 0) {
      bufferPool.releaseBuffer(bufferType, buffer);
    }
    return usages;
  }

  @VisibleForTesting
  public void setBufferForTestingOnly(final ByteBuffer newBufferForTesting) {
    buffer = newBufferForTesting;
  }

  private void addReferenceAfterLock() throws IOException {
    try {
      addReference();
    } catch (final IOException e) {
      lock.unlock();
      throw e;
    }
  }

  private void throwClosed() throws IOException {
    throw new IOException("NioSslEngine has been closed");
  }

}
