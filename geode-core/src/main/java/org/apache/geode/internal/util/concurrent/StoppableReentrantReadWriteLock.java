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
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain, as explained at http://creativecommons.org/licenses/publicdomain
 */

package org.apache.geode.internal.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.Assert;

/**
 * Instances of {@link java.util.concurrent.locks.ReentrantReadWriteLock} that respond to
 * cancellation
 *
 */
public class StoppableReentrantReadWriteLock implements /* ReadWriteLock, */ java.io.Serializable {
  private static final long serialVersionUID = -1185707921434766946L;

  /**
   * The underlying read lock
   */
  private final transient StoppableReadLock readLock;

  /**
   * the underlying write lock
   */
  private final transient StoppableWriteLock writeLock;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

  /**
   * Create a new instance
   *
   * @param stopper the cancellation criterion
   */
  public StoppableReentrantReadWriteLock(CancelCriterion stopper) {
    this(new ReentrantReadWriteLock(), stopper);
  }

  protected StoppableReentrantReadWriteLock(ReadWriteLock lock, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    readLock = new StoppableReadLock(lock, stopper);
    writeLock = new StoppableWriteLock(lock, stopper);
  }

  /**
   * @return the read lock
   */
  public StoppableReadLock readLock() {
    return readLock;
  }

  /**
   * @return the write lock
   */
  public StoppableWriteLock writeLock() {
    return writeLock;
  }

  /**
   * read locks that are stoppable
   */
  public static class StoppableReadLock {

    private final Lock lock;
    private final CancelCriterion stopper;

    /**
     * Create a new read lock from the given lock
     *
     * @param lock the lock to be used
     * @param stopper the cancellation criterion
     */
    StoppableReadLock(ReadWriteLock lock, CancelCriterion stopper) {
      this.lock = lock.readLock();
      this.stopper = stopper;
    }

    public void lock() {
      boolean interrupted = Thread.interrupted();
      try {
        for (;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } // for
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
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
     * @return true if the lock was acquired
     */
    public boolean tryLock() {
      stopper.checkCancelInProgress(null);
      return lock.tryLock();
    }

    /**
     * @param timeout how long to wait
     * @return true if the lock was acquired
     */
    public boolean tryLock(long timeout) throws InterruptedException {
      stopper.checkCancelInProgress(null);
      return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    public void unlock() {
      lock.unlock();
    }

    // /**
    // * @return the new condition
    // */
    // public StoppableCondition newCondition() {
    // return new StoppableCondition(lock.newCondition(), stopper);
    // }
  }

  public static class StoppableWriteLock {

    /**
     * The underlying write lock
     */
    private final Lock lock;

    /**
     * the cancellation criterion
     */
    private final CancelCriterion stopper;

    /**
     * Create a new instance
     *
     * @param lock the underlying lock
     */
    public StoppableWriteLock(ReadWriteLock lock, CancelCriterion stopper) {
      this.lock = lock.writeLock();
      this.stopper = stopper;
    }

    public void lock() {
      boolean interrupted = Thread.interrupted();
      try {
        for (;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } // for
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
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
     * @return true if the lock was acquired
     */
    public boolean tryLock() {
      stopper.checkCancelInProgress(null);
      return lock.tryLock();
    }

    /**
     * @param timeout how long to wait
     * @return true if the lock was required
     */
    public boolean tryLock(long timeout) throws InterruptedException {
      stopper.checkCancelInProgress(null);
      return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    public void unlock() {
      lock.unlock();
    }

    // /**
    // * @return the new condition
    // */
    // public StoppableCondition newCondition() {
    // return new StoppableCondition(lock.newCondition(), stopper);
    // }

  }
}
