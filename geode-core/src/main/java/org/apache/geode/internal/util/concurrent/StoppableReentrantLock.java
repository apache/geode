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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.Assert;

/**
 * Instances of {@link java.util.concurrent.locks.Lock} that respond to cancellations
 *
 */
public class StoppableReentrantLock {
  /**
   * the underlying lock
   */
  private final ReentrantLock lock;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

  /**
   * the cancellation criterion
   */
  private final CancelCriterion stopper;

  /**
   * Create a new instance with the given cancellation criterion
   *
   * @param stopper the cancellation criterion
   */
  public StoppableReentrantLock(CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    lock = new ReentrantLock();
    this.stopper = stopper;
  }

  /**
   * Create a new instance with given fairness and cancellation criterion
   *
   * @param fair whether to be fair
   * @param stopper the cancellation criterion
   */
  public StoppableReentrantLock(boolean fair, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    this.stopper = stopper;
    lock = new ReentrantLock();
  }


  public void lock() {
    for (;;) {
      boolean interrupted = Thread.interrupted();
      try {
        lockInterruptibly();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for
  }

  public void lockInterruptibly() throws InterruptedException {
    for (;;) {
      stopper.checkCancelInProgress(null);
      if (lock.tryLock(RETRY_TIME, TimeUnit.MILLISECONDS)) {
        break;
      }
    }
  }

  /**
   * @return true if the lock is acquired
   */
  public boolean tryLock() {
    stopper.checkCancelInProgress(null);
    return lock.tryLock();
  }

  /**
   * @param timeoutMs how long to wait in milliseconds
   * @return true if the lock was acquired
   */
  public boolean tryLock(long timeoutMs) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public void unlock() {
    lock.unlock();
  }

  /**
   * @return the new stoppable condition
   */
  public StoppableCondition newCondition() {
    return new StoppableCondition(lock.newCondition(), stopper);
  }

  /**
   * @return true if it is held
   */
  public boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }
}
