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
package org.apache.geode.internal.util.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * A special purpose semaphore that allows reentrancy. With this semaphore, a thread will only
 * acquire a real permit the first time it calls acquire. After that, the thread can call acquire
 * repeatedly, and it won't affect the semaphore count until the thread calls release the same
 * number of times as acquire.
 *
 * This semaphore currently only supports a thread acquiring and releasing a single permit at a
 * time.
 *
 * If a thread does not hold the semaphore, but still decrements it, then it will increase the
 * permits of the semaphore, just like a regular semaphore.
 *
 * This semaphore is useful for allowing only a limited number of threads to enter a block of code,
 * while allowing a single thread to try to enter that block several times in it's call stack.
 *
 */
public class ReentrantSemaphore extends Semaphore {
  private final ThreadLocal<Integer> holdCount = new ThreadLocal<>();

  public ReentrantSemaphore(int permits) {
    super(permits);
  }

  public ReentrantSemaphore(int permits, boolean fair) {
    super(permits, fair);
  }

  @Override
  public void acquire() throws InterruptedException {
    if (incHoldCount()) {
      super.acquire();
    }
  }

  @Override
  public void acquireUninterruptibly() {
    if (incHoldCount()) {
      super.acquireUninterruptibly();
    }
  }

  @Override
  public boolean tryAcquire() {
    if (incHoldCount()) {
      boolean result = super.tryAcquire();
      if (!result) {
        decHoldCount();
      }
      return result;
    } else {
      return true;
    }
  }

  @Override
  public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
    if (incHoldCount()) {
      boolean result = super.tryAcquire();
      if (!result) {
        decHoldCount();
      }
      return result;
    } else {
      return true;
    }
  }

  @Override
  public void release() {
    if (decHoldCount()) {
      super.release();
    }
  }

  @Override
  public void acquire(int permits) throws InterruptedException {
    throw new UnsupportedOperationException(
        "ReentrantSemaphore doesn't support a single thread using more than one permit");
  }

  @Override
  public void acquireUninterruptibly(int permits) {
    throw new UnsupportedOperationException(
        "ReentrantSemaphore doesn't support a single thread using more than one permit");
  }

  @Override
  public boolean tryAcquire(int permits) {
    throw new UnsupportedOperationException(
        "ReentrantSemaphore doesn't support a single thread using more than one permit");
  }

  @Override
  public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException(
        "ReentrantSemaphore doesn't support a single thread using more than one permit");
  }

  @Override
  public void release(int permits) {
    throw new UnsupportedOperationException(
        "ReentrantSemaphore doesn't support a single thread using more than one permit");
  }

  private boolean incHoldCount() {
    Integer count = holdCount.get();
    if (count != null) {
      holdCount.set(Integer.valueOf(count + 1));
      return false;
    }
    {
      holdCount.set(Integer.valueOf(1));
      return true;
    }
  }

  private boolean decHoldCount() {
    Integer count = holdCount.get();
    if (count == null) {
      return true;
    }
    if (count == 1) {
      holdCount.remove();
      return true;
    } else {
      holdCount.set(Integer.valueOf(count - 1));
      return false;
    }
  }

  public boolean tryAcquireMs(long timeout) throws InterruptedException {
    return tryAcquire(timeout, TimeUnit.MILLISECONDS);
  }

  public boolean tryAcquireMs(int permits, long timeout) throws InterruptedException {
    return tryAcquire(permits, TimeUnit.MILLISECONDS);
  }
}
