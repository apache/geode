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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.Assert;

/**
 * This class is a "stoppable" cover for {@link CountDownLatch}.
 */
public class StoppableCountDownLatch {

  static final String RETRY_TIME_MILLIS_PROPERTY = GEMFIRE_PREFIX + "stoppable-retry-interval";
  static final long RETRY_TIME_MILLIS_DEFAULT = 2000;

  static final long RETRY_TIME_NANOS = MILLISECONDS.toNanos(
      Long.getLong(RETRY_TIME_MILLIS_PROPERTY, RETRY_TIME_MILLIS_DEFAULT));

  private final CountDownLatch delegate;

  private final CancelCriterion stopper;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private final long retryIntervalNanos;

  private final NanoTimer nanoTimer;

  /**
   * @param stopper the CancelCriterion to check before awaiting
   * @param count the number of times {@link #countDown} must be invoked before threads can pass
   *        through {@link #await()}
   *
   * @throws IllegalArgumentException if {@code count} is negative
   */
  public StoppableCountDownLatch(final CancelCriterion stopper, final int count) {
    this(stopper, count, RETRY_TIME_NANOS, System::nanoTime);
  }

  StoppableCountDownLatch(final CancelCriterion stopper, final int count,
      final long retryIntervalNanos, final NanoTimer nanoTimer) {
    Assert.assertTrue(stopper != null);
    delegate = new CountDownLatch(count);
    this.stopper = stopper;
    this.retryIntervalNanos = retryIntervalNanos;
    this.nanoTimer = nanoTimer;
  }

  public void await() throws InterruptedException {
    do {
      stopper.checkCancelInProgress(null);
    } while (!delegate.await(retryIntervalNanos, NANOSECONDS));
  }

  public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    long timeoutNanos = unit.toNanos(timeout);
    if (timeoutNanos > retryIntervalNanos) {
      return awaitWithCheck(timeoutNanos);
    }
    return delegate.await(timeoutNanos, NANOSECONDS);
  }

  /**
   * @param timeoutMillis how long to wait in milliseconds
   *
   * @return true if it was unlatched
   */
  public boolean await(final long timeoutMillis) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    long timeoutNanos = MILLISECONDS.toNanos(timeoutMillis);
    if (timeoutNanos > retryIntervalNanos) {
      return awaitWithCheck(timeoutNanos);
    }
    return delegate.await(timeoutNanos, NANOSECONDS);
  }

  public void countDown() {
    delegate.countDown();
  }

  public long getCount() {
    return delegate.getCount();
  }

  @Override
  public String toString() {
    return "(Stoppable) " + delegate;
  }

  long retryIntervalNanos() {
    return retryIntervalNanos;
  }

  private boolean awaitWithCheck(final long timeoutNanos) throws InterruptedException {
    long startNanos = nanoTimer.nanoTime();
    boolean unlatched;
    do {
      stopper.checkCancelInProgress(null);
      unlatched = delegate.await(retryIntervalNanos, NANOSECONDS);
    } while (!unlatched && nanoTimer.nanoTime() - startNanos < timeoutNanos);
    return unlatched;
  }

  @FunctionalInterface
  interface NanoTimer {
    long nanoTime();
  }
}
