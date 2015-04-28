/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * An <code>AdminException</code> is thrown when administration or monitoring
 * of GemFire fails. 
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AdminException extends GemFireCheckedException {
private static final long serialVersionUID = 879398950879472021L;

  /**
   * Constructs a new exception with <code>null</code> as its detail message.
   * The cause is not initialized, and may subsequently be initialized by a
   * call to {@link Throwable#initCause}.
   */
  public AdminException() {
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
  public AdminException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and
   * cause.  <p>Note that the detail message associated with
   * <code>cause</code> is <i>not</i> automatically incorporated in
   * this exception's detail message.
   *
   * @param  message the detail message (which is saved for later retrieval
   *         by the {@link #getMessage()} method).
   * @param  cause the cause (which is saved for later retrieval by the
   *         {@link #getCause()} method).  (A <tt>null</tt> value is
   *         permitted, and indicates that the cause is nonexistent or
   *         unknown.)
   */
  public AdminException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified cause.
   * The detail
   * message will be <tt>(cause==null ? null : cause.toString())</tt> (which
   * typically contains the class and detail message of <tt>cause</tt>).
   * This constructor is useful for exceptions that are little more than
   * wrappers for other throwables (for example, {@link
   * java.security.PrivilegedActionException}).
   *
   * @param  cause the cause (which is saved for later retrieval by the
   *         {@link #getCause()} method).  (A <tt>null</tt> value is
   *         permitted, and indicates that the cause is nonexistent or
   *         unknown.)
   */
  public AdminException(Throwable cause) {
    super(cause);
  }
    
}
