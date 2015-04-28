/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;

/**
 * Instances of {@link java.util.concurrent.locks.ReentrantReadWriteLock}
 * that respond to cancellation
 * @author jpenney
 *
 */
public class StoppableReentrantReadWriteLock implements /* ReadWriteLock, */ java.io.Serializable  {
  private static final long serialVersionUID = -1185707921434766946L;
  
  /**
   * The underlying read lock
   */
  transient private final StoppableReadLock readLock;
  
  /**
   * the underlying write lock
   */
  transient private final StoppableWriteLock writeLock;
  
  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

  /**
   * Create a new instance
   * @param stopper the cancellation criterion
   */
  public StoppableReentrantReadWriteLock(CancelCriterion stopper) {
    this(new ReentrantReadWriteLock(), stopper);
  }

  protected StoppableReentrantReadWriteLock(ReadWriteLock lock, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    this.readLock = new StoppableReadLock(lock, stopper);
    this.writeLock = new StoppableWriteLock(lock, stopper);
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
   * @author jpenney
   */
  static public class StoppableReadLock {

    private final Lock lock;
    private final CancelCriterion stopper;
    
    /**
     * Create a new read lock from the given lock
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
          }
          catch (InterruptedException e) {
            interrupted = true;
          }
        } // for
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    /**
     * @throws InterruptedException
     */
    public void lockInterruptibly() throws InterruptedException {
      for (;;) {
        stopper.checkCancelInProgress(null);
        if (lock.tryLock(RETRY_TIME, TimeUnit.MILLISECONDS))
          break;
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
     * @throws InterruptedException
     */
    public boolean tryLock(long timeout) throws InterruptedException {
      stopper.checkCancelInProgress(null);
      return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    public void unlock() {
      lock.unlock();
    }

//     /**
//      * @return the new condition
//      */
//     public StoppableCondition newCondition() {
//       return new StoppableCondition(lock.newCondition(), stopper);
//     }
  }
  
  static public class StoppableWriteLock {
    
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
     * @param lock the underlying lock
     * @param stopper
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
          }
          catch (InterruptedException e) {
            interrupted = true;
          }
        } // for
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    /**
     * @throws InterruptedException
     */
    public void lockInterruptibly() throws InterruptedException {
      for (;;) {
        stopper.checkCancelInProgress(null);
        if (lock.tryLock(RETRY_TIME, TimeUnit.MILLISECONDS))
          break;
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
     * @throws InterruptedException
     */
    public boolean tryLock(long timeout) throws InterruptedException {
      stopper.checkCancelInProgress(null);
      return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     */
    public void unlock() {
      lock.unlock();
    }

//     /**
//      * @return the new condition
//      */
//     public StoppableCondition newCondition() {
//       return new StoppableCondition(lock.newCondition(), stopper);
//     }
    
  }
}
