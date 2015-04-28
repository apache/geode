/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;

/**
 * This class is a "stoppable" cover for {@link CountDownLatch}.
 * @author jpenney
 */
public class StoppableCountDownLatch {

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  static final long RETRY_TIME = Long.getLong("gemfire.stoppable-retry-interval", 2000).longValue();

  
  /**
   * The underlying latch
   */
  private final CountDownLatch latch;

  /**
   * The cancellation criterion
   */
  private final CancelCriterion stopper;
  
  /**
   * @param count the number of times {@link #countDown} must be invoked
   *        before threads can pass through {@link #await()}
   * @throws IllegalArgumentException if {@code count} is negative
   */
  public StoppableCountDownLatch(CancelCriterion stopper, int count) {
      Assert.assertTrue(stopper != null);
      this.latch = new CountDownLatch(count);
      this.stopper = stopper;
  }

  /**
   * @throws InterruptedException
   */
  public void await() throws InterruptedException {
      for (;;) {
        stopper.checkCancelInProgress(null);
        if (latch.await(RETRY_TIME, TimeUnit.MILLISECONDS)) {
          break;
        }
      }
  }

  /**
   * @param msTimeout how long to wait in milliseconds
   * @return true if it was unlatched
   * @throws InterruptedException
   */
  public boolean await(long msTimeout) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return latch.await(msTimeout, TimeUnit.MILLISECONDS);
  }

  public synchronized void countDown() {
    latch.countDown();
  }

  /**
   * @return the current count
   */
  public long getCount() {
    return latch.getCount();
  }

  /**
   * @return a string identifying this latch, as well as its state
   */
  @Override
  public String toString() {
      return "(Stoppable) " + latch.toString();
  }
}
