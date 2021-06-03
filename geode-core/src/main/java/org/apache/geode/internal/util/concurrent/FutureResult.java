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

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.geode.CancelCriterion;

/**
 * Represents a Future without a background thread. Instead of a Runnable that will set the value,
 * some known thread will set the value in the future by calling the set method. Also supports the
 * case where the "future is now" when the value is known at construction time.
 *
 * Cancelling this future sets the state to cancelled and causes threads waiting on get to proceed
 * with a CancellationException.
 *
 */
public class FutureResult<V> implements Future<V> {
  private final StoppableCountDownLatch latch;
  private V value;
  private volatile boolean isCancelled = false;

  /** Creates a new instance of FutureResult */
  public FutureResult(CancelCriterion crit) {
    latch = new StoppableCountDownLatch(crit, 1);
  }

  /** Creates a new instance of FutureResult with the value available immediately */
  public FutureResult(V value) {
    this.value = value;
    latch = null;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (isCancelled) {
      return false; // already cancelled
    }
    isCancelled = true;
    if (latch != null) {
      latch.countDown();
    }
    return true;
  }

  @Override
  public V get() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException(); // check in case latch is null
    }
    if (isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    if (latch != null) {
      latch.await();
    }
    if (isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    return value;
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (Thread.interrupted()) {
      throw new InterruptedException(); // check in case latch is null
    }
    if (isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    if (latch != null) {
      if (!latch.await(unit.toMillis(timeout))) {
        throw new TimeoutException();
      }
    }
    return value;
  }

  @Override
  public boolean isCancelled() {
    return isCancelled;
  }

  @Override
  public boolean isDone() {
    return latch == null || latch.getCount() == 0L || isCancelled;
  }

  public void set(V value) {
    this.value = value;
    if (latch != null) {
      latch.countDown();
    }
  }
}
