/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed;

/**
 * A <code>LockNotHeldException</code> is thrown when attempting
 * unlock a lock that is not held by the thread performing the unlock.
 *
 * @author    Kirk Lund
 * @since     4.0
 */
public class LockNotHeldException extends IllegalStateException {
private static final long serialVersionUID = 3827860179805916215L;
  /**
   * Constructs a new exception with <code>null</code> as its detail message.
   * The cause is not initialized, and may subsequently be initialized by a
   * call to {@link Throwable#initCause}.
   */
  public LockNotHeldException() {
    super();
  }

  /**
   * Constructs a new exception with the specified detail message.  The
   * cause is not initialized, and may subsequently be initialized by
   * a call to {@link Throwable#initCause}.
   *
   * @param   message   the detail message. The detail message is saved for 
   *          later retrieval by the {@link #getMessage()} method.
   */
  public LockNotHeldException(String message) {
    super(message);
  }

}

