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

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.Assert;

/**
 * Extends the CountDownLatch with the ability to also countUp.
 * <p>
 * Based on the original Doug Lea backport implementation of CountDownLatch.
 *
 * @see java.util.concurrent.CountDownLatch
 */
public class StoppableCountDownOrUpLatch {

  private int count_;

  /**
   * The cancellation criterion
   */
  private CancelCriterion stopper;

  /**
   * Constructs a <code>CountDownLatch</code> initialized with the given count.
   *
   * @param count the number of times {@link #countDown} must be invoked before threads can pass
   *        through {@link #await()}
   * @throws IllegalArgumentException if <code>count</count> is negative
   */
  public StoppableCountDownOrUpLatch(CancelCriterion stopper, int count) {
    Assert.assertTrue(stopper != null);
    if (count < 0)
      throw new IllegalArgumentException(
          "count < 0");
    this.stopper = stopper;
    this.count_ = count;
  }

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is {@linkplain Thread#interrupt interrupted}.
   *
   * <p>
   * If the current count is zero then this method returns immediately.
   *
   * <p>
   * If the current count is greater than zero then the current thread becomes disabled for thread
   * scheduling purposes and lies dormant until one of two things happen:
   * <ul>
   * <li>The count reaches zero due to invocations of the {@link #countDown} method; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread.
   * </ul>
   *
   * <p>
   * If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's interrupted status is
   * cleared.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void await() throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    // Modified to use inner primitive repeatedly, checking
    // for cancellation
    for (;;) {
      stopper.checkCancelInProgress(null);
      if (await(StoppableCountDownLatch.RETRY_TIME))
        break;
    }
  }

  private static final long NANOS_PER_MS = 1000000;

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is {@linkplain Thread#interrupt interrupted}, or the specified waiting time elapses.
   *
   * <p>
   * If the current count is zero then this method returns immediately with the value
   * <code>true</code>.
   *
   * <p>
   * If the current count is greater than zero then the current thread becomes disabled for thread
   * scheduling purposes and lies dormant until one of three things happen:
   * <ul>
   * <li>The count reaches zero due to invocations of the {@link #countDown} method; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread; or
   * <li>The specified waiting time elapses.
   * </ul>
   *
   * <p>
   * If the count reaches zero then the method returns with the value <code>true</code>.
   *
   * <p>
   * If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's interrupted status is
   * cleared.
   *
   * <p>
   * If the specified waiting time elapses then the value <code>false</code> is returned. If the
   * time is less than or equal to zero, the method will not wait at all.
   *
   * @param msTimeout the maximum time to wait in milliseconds
   * @return <code>true</code> if the count reached zero and <code>false</code> if the waiting time
   *         elapsed before the count reached zero
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public boolean await(long msTimeout) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    long nanos = msTimeout * NANOS_PER_MS; // millis to nanos
    synchronized (this) {
      if (count_ <= 0)
        return true;
      else if (nanos <= 0)
        return false;
      else {
        long deadline = System.nanoTime() + nanos;
        for (;;) {
          stopper.checkCancelInProgress(null);
          wait(nanos / NANOS_PER_MS, (int) (nanos % NANOS_PER_MS));
          if (count_ <= 0)
            return true;
          else {
            nanos = deadline - System.nanoTime();
            if (nanos <= 0)
              return false;
          }
        }
      }
    }
  }

  /**
   * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
   *
   * <p>
   * If the current count is greater than zero then it is decremented. If the new count is zero then
   * all waiting threads are re-enabled for thread scheduling purposes.
   *
   * <p>
   * If the current count equals zero then nothing happens.
   */
  public synchronized void countDown() {
    if (count_ == 0)
      return;
    if (--count_ == 0)
      notifyAll();
  }

  /**
   * Returns the current count.
   *
   * <p>
   * This method is typically used for debugging and testing purposes.
   *
   * @return the current count
   */
  public long getCount() {
    return count_;
  }

  /**
   * Returns a string identifying this latch, as well as its state. The state, in brackets, includes
   * the String <code>"Count ="</code> followed by the current count.
   *
   * @return a string identifying this latch, as well as its state
   */
  @Override
  public String toString() {
    return super.toString() + "[Count = " + getCount() + "]";
  }

  /**
   * [GemStone addition]
   */
  public synchronized void countUp() {
    this.count_++;
  }

  /**
   * [GemStone addition]
   */
  public synchronized long getCountSync() {
    return getCount();
  }
}
