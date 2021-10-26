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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.net.BufferPool.BufferType;

/**
 * Produces (via {@link #open()}) an {@link ByteBufferSharing} meant to used only within a
 * try-with-resources block. The resource controls access to a secondary resource
 * (via {@link ByteBufferSharing#getBuffer()}) within the scope of try-with-resources.
 * Neither the object returned by {@link #open()}, nor the object returned by invoking
 * {@link ByteBufferSharing#getBuffer()} on that object may be used outside the scope of
 * try-with-resources.
 */
public class ByteBufferVendor {

  static class OpenAttemptTimedOut extends Exception {
  }

  private interface ByteBufferSharingInternal extends ByteBufferSharing {
    void releaseBuffer();
  }

  private final Lock lock = new ReentrantLock();
  private final AtomicBoolean isDestructed = new AtomicBoolean(false);
  private final AtomicInteger counter = new AtomicInteger(1);
  // the object referenced by sharing is guarded by lock
  private final ByteBufferSharingInternal sharing;

  /*
   * These constructors are for use only by the owner of the shared resource.
   *
   * A resource owner must invoke {@link #open()} once for each reference that escapes (is passed
   * to an external object or is returned to an external caller.)
   *
   * Constructors acquire no locks. The reference count will be 1 after a constructor
   * completes.
   */

  /**
   * When you have a ByteBuffer available before construction, use this constructor.
   *
   * @param bufferArg is the ByteBuffer
   * @param bufferType needed for freeing the buffer later
   * @param bufferPool needed for freeing the buffer later
   */
  public ByteBufferVendor(@NotNull final ByteBuffer bufferArg,
      final BufferType bufferType,
      final BufferPool bufferPool) {
    sharing = new ByteBufferSharingInternalImpl(bufferArg, bufferType, bufferPool);
  }

  /**
   * This method is for use only by the owner of the shared resource. It's used for handing out
   * references to the shared resource. So it does reference counting and also acquires a lock.
   *
   * Resource owners call this method as the last thing before returning a reference to the caller.
   * That caller binds that reference to a variable in a try-with-resources statement and relies on
   * the AutoCloseable protocol to invoke {@link AutoCloseable#close()} on the object at
   * the end of the block.
   */
  public ByteBufferSharing open() throws IOException {
    lock.lock();
    addReferenceAfterLock();
    return sharing;
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
    return sharing;
  }

  /**
   * The destructor. Called by the resource owner to undo the work of the constructor.
   */
  public void destruct() {
    if (isDestructed.compareAndSet(false, true)) {
      dropReference();
    }
  }

  private void exposingResource() throws IOException {
    if (isDestructed.get()) {
      throwClosed();
    }
  }

  private void close() {
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
      sharing.releaseBuffer();
    }
    return usages;
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

  private class ByteBufferSharingInternalImpl implements ByteBufferSharingInternal {

    /*
     * mutable because in general our ByteBuffer may need to be resized (grown or compacted)
     * no concurrency concerns since ByteBufferSharingNotNull is guarded by ByteBufferVendor.lock
     */
    private ByteBuffer buffer;
    private final BufferType bufferType;
    private final BufferPool bufferPool;

    public ByteBufferSharingInternalImpl(final ByteBuffer buffer,
        final BufferType bufferType,
        final BufferPool bufferPool) {
      Objects.requireNonNull(buffer);
      this.buffer = buffer;
      this.bufferType = bufferType;
      this.bufferPool = bufferPool;
    }

    @Override
    public ByteBuffer getBuffer() throws IOException {
      exposingResource();
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
      ByteBufferVendor.this.close();
    }

    /*
     * NOTE WELL: it may appear that this method is available outside this package but it is not!
     * Unlike the methods above, this one is overridden from the private ByteBufferSharingInternal
     * interface defined in our outer class ByteBufferVendor. It is called only by
     * ByteBufferVendor.dropReference(), itself a private method.
     *
     * The whole point of this framework is to control release of buffers. We don't let any
     * code outside this file cause them (directly) to be released.
     */
    @Override
    public void releaseBuffer() {
      bufferPool.releaseBuffer(bufferType, buffer);
    }
  }

  @VisibleForTesting
  public void setBufferForTestingOnly(final ByteBuffer newBufferForTesting) {
    ((ByteBufferSharingInternalImpl) sharing).buffer = newBufferForTesting;
  }
}
