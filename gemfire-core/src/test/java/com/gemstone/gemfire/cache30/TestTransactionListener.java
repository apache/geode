/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

/**
 * A <code>TransactionListener</code> used in testing.  Its callback methods
 * are implemented to throw {@link UnsupportedOperationException}
 * unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 * @author Mitch Thomas
 * @since 4.0
 */
public abstract class TestTransactionListener extends TestCacheCallback
  implements TransactionListener {

  public final void afterCommit(TransactionEvent event) {
    this.invoked = true;
    try {
      afterCommit2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterCommit2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void afterFailedCommit(TransactionEvent event) {
    this.invoked = true;
    try {
      afterFailedCommit2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterFailedCommit2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }


  public final void afterRollback(TransactionEvent event) {
    this.invoked = true;
    try {
      afterRollback2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterRollback2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

}
