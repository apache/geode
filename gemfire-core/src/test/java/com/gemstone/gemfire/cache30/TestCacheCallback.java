/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * An abstract superclass of implementation of GemFire cache callbacks
 * that are used for testing.
 *
 * @see #wasInvoked
 *
 * @author David Whitlock
 * @since 3.0
 */
public abstract class TestCacheCallback implements CacheCallback {
  // differentiate between callback being closed and callback
  // event methods being invoked
  private volatile boolean isClosed = false;
  
  /** Was a callback event method invoked? */
  volatile boolean invoked = false;
  
  volatile protected Throwable callbackError = null;

  /**
   * Returns whether or not one of this <code>CacheListener</code>
   * methods was invoked.  Before returning, the <code>invoked</code>
   * flag is cleared.
   */
  public boolean wasInvoked() {
    checkForError();
    boolean value = this.invoked;
    if (value) {
      this.invoked = false;
    }
    return value;
  }
  /**
   * Waits up to timeoutMs milliseconds for the listener to be invoked.
   * Calls wasInvoked and returns its value
   */
  public boolean waitForInvocation(int timeoutMs) {
    return waitForInvocation(timeoutMs, 200);
  }
  public boolean waitForInvocation(int timeoutMs, long interval) {
    if (!this.invoked) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return invoked;
        }
        public String description() {
          return "listener was never invoked";
        }
      };
      DistributedTestCase.waitForCriterion(ev, timeoutMs, interval, true);
    }
    return wasInvoked();
  }
  
  public boolean isClosed() {
    checkForError();
    return this.isClosed;
  }

  public final void close() {
    this.isClosed = true;
    close2();
  }

  /**
   * This method will do nothing.  Note that it will not throw an
   * exception. 
   */
  public void close2() {

  }
  
  private void checkForError() {
    if (this.callbackError != null) {
      AssertionError  error = new AssertionError("Exception occurred in callback");
      error.initCause(this.callbackError);
      throw error;
    }
  }
}
