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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An {@link AutoCloseable} meant to be acquired in a try-with-resources statement. The resource (a
 * {@link ByteBuffer}) is available (for reading and modification) in the scope of the
 * try-with-resources. Collaborates with {@link ByteBufferReferencing} to ensure final dereference
 * returns the {@link ByteBuffer} to the pool.
 */
class ByteBufferSharingImpl implements ByteBufferSharing {
  private static final Logger logger = LogService.getLogger();

  static class LockAttemptTimedOut extends Exception {
  }

  private final Lock lock;
  private final ByteBufferReferencing referencing;

  /**
   * This constructor is for use only by the owner of the shared resource (a {@link ByteBuffer}).
   *
   * A resource owner must invoke {@link #alias()} once for each reference that escapes (is passed
   * to an external object or is returned to an external caller.)
   *
   * This constructor acquires no lock and does not modify the reference count through the {@link
   * ByteBufferReferencing} (passed in here.) Usually, the referencing object will have been set to
   * a reference of 1 before calling this method.
   */
  ByteBufferSharingImpl(final ByteBufferReferencing referencing) {
    this(new ReentrantLock(), referencing);
  }

  /**
   * This method is for use only by the owner of the shared resource. It's used for handing out
   * references to the shared resource. So it does reference counting and also acquires a lock.
   *
   * Resource owners call this method as the last thing before returning a reference to the caller.
   * That caller binds that reference to a variable in a try-with-resources statement and relies on
   * the AutoCloseable protocol to invoke close() on the object at the end of the block.
   */
  ByteBufferSharing alias() {
    lock.lock();
    referencing.addReference();
    return this;
  }

  ByteBufferSharing alias(final long time, final TimeUnit unit) throws LockAttemptTimedOut {
    try {
      if (!lock.tryLock(time, unit)) {
        throw new LockAttemptTimedOut();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LockAttemptTimedOut();
    }
    final int refcount = referencing.addReference();
    return this;
  }

  private ByteBufferSharingImpl(final Lock lock,
      final ByteBufferReferencing referencing) {
    this.lock = lock;
    this.referencing = referencing;
  }

  @Override
  public ByteBuffer getBuffer() throws IOException {
    return referencing.getBuffer();
  }

  @Override
  public void close() {
    referencing.dropReference();
    lock.unlock();
  }

}
