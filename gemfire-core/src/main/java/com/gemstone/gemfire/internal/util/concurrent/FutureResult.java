/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Represents a Future without a background thread. Instead of a Runnable
 * that will set the value, some known thread will set the value in the
 * future by calling the set method. Also supports the case where the
 * "future is now" when the value is known at construction time.
 *
 * Cancelling this future sets the state to cancelled and causes threads
 * waiting on get to proceed with a CancellationException.
 *
 * @author Eric Zoerner
 */
public class FutureResult implements Future {
  private final StoppableCountDownLatch latch;
  private Object value ;
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
   if (this.isCancelled) return false; //already cancelled
   this.isCancelled = true;
   if (this.latch != null) this.latch.countDown();
   return true;
  }

  public Object get() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // check in case latch is null
    if (this.latch != null) this.latch.await();
    if (this.isCancelled) {
      throw new CancellationException(LocalizedStrings.FutureResult_FUTURE_WAS_CANCELLED.toLocalizedString());
    }
    return this.value;
  }

  public Object get(long timeout, TimeUnit unit)
  throws InterruptedException, TimeoutException {
    if (Thread.interrupted()) throw new InterruptedException(); // check in case latch is null
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
    if (this.latch != null) this.latch.countDown();
  }
}
