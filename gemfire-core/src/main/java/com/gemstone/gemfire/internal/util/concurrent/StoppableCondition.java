/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
  File: ConditionVariable.java
  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.
  History:
  Date       Who                What
  11Jun1998  dl               Create public version
 */

package com.gemstone.gemfire.internal.util.concurrent;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;

/**
 * This class is functionally equivalent to {@link java.util.concurrent.locks.Condition};
 * however, it does not implement the interface, in an attempt to encourage
 * GemFire API writers to refer to this "stoppable" version instead.
 * <p>
 * It is implemented as a strict "cover" for a genuine {@link java.util.concurrent.locks.Condition}.
 * 
 * @author jpenney
 */
public class StoppableCondition implements /* Condition, */ java.io.Serializable {
    private static final long serialVersionUID = -7091681525970431937L;

    /** The underlying condition **/
    private final Condition condition;
    
    /** The cancellation object */
    private final CancelCriterion stopper;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

    /**
     * Create a new StoppableCondition based on given condition and
     * cancellation criterion
     * @param c the underlying condition
     **/
    StoppableCondition(Condition c, CancelCriterion stopper) {
        Assert.assertTrue(stopper != null);
        this.condition = c;
        this.stopper = stopper;
    }

    public void awaitUninterruptibly() {
      for (;;) {
        boolean interrupted = Thread.interrupted();
        try {
          await();
          break;
        }
        catch (InterruptedException e) {
          interrupted = true;
        }
        finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
      }
    }

    public void await() throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException();
      for (;;) {
        stopper.checkCancelInProgress(null);
        if (await(RETRY_TIME))
          break;
      }
    }

    public boolean await(long timeoutMs) throws InterruptedException {
        stopper.checkCancelInProgress(null);
        return condition.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean awaitUntil(Date deadline) throws InterruptedException {
      stopper.checkCancelInProgress(null);
      return condition.awaitUntil(deadline);
    }

    public synchronized void signal() {
      condition.signal();
    }

    public synchronized void signalAll() {
      condition.signalAll();
    }
}
