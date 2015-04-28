/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This is a type of {@link StoppableReentrantLock} that does not allow
 * recursion
 * 
 */
public class StoppableNonReentrantLock extends StoppableReentrantLock
{
  /**
   * Creates an instance.
   * @param stopper the cancellation object
   */
  public StoppableNonReentrantLock(CancelCriterion stopper) {
    super(stopper);
  }

  /**
   * Creates an instance with the
   * given fairness policy.
   *
   * @param fair <code>true</code> if this lock should use a fair ordering policy
   * @param stopper the cancellation object
   */
  public StoppableNonReentrantLock(boolean fair, CancelCriterion stopper) {
    super(fair, stopper);
  }

  /**
   * @throws IllegalStateException if reentry is detected
   */
  private void checkForRentry() {
    if (isHeldByCurrentThread()) {
      throw new IllegalStateException(LocalizedStrings.StoppableNonReentrantLock_LOCK_REENTRY_IS_NOT_ALLOWED.toLocalizedString());
    }
  }
  
  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public void lock() {
    checkForRentry();
    super.lock();
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public void lockInterruptibly() throws InterruptedException {
    checkForRentry();
    super.lockInterruptibly();
  }
  
  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public boolean tryLock() {
    checkForRentry();
    return super.tryLock();
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public boolean tryLock(long timeoutMs) throws InterruptedException {
    checkForRentry();
    return super.tryLock(timeoutMs);
  }
}
