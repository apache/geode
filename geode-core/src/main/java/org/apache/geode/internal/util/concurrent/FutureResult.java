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
public class FutureResult implements Future {
  private final StoppableCountDownLatch latch;
  private Object value;
  private volatile boolean isCancelled = false;

  /** Creates a new instance of FutureResult */
  public FutureResult(CancelCriterion crit) {
    this.latch = new StoppableCountDownLatch(crit, 1);
  }

  /** Creates a new instance of FutureResult with the value available immediately */
  public FutureResult(Object value) {
    this.value = value;
    this.latch = null;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    if (this.isCancelled)
      return false; // already cancelled
    this.isCancelled = true;
    if (this.latch != null)
      this.latch.countDown();
    return true;
  }

  public Object get() throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException(); // check in case latch is null
    if (this.isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    if (this.latch != null)
      this.latch.await();
    if (this.isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    return this.value;
  }

  public Object get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (Thread.interrupted())
      throw new InterruptedException(); // check in case latch is null
    if (this.isCancelled) {
      throw new CancellationException(
          "Future was cancelled");
    }
    if (this.latch != null) {
      if (!this.latch.await(unit.toMillis(timeout))) {
        throw new TimeoutException();
      }
    }
    return this.value;
  }

  public boolean isCancelled() {
    return this.isCancelled;
  }

  public boolean isDone() {
    return this.latch == null || this.latch.getCount() == 0L || this.isCancelled;
  }

  public void set(Object value) {
    this.value = value;
    if (this.latch != null)
      this.latch.countDown();
  }
}
