/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown when a cache transaction fails to register with the
 * <code>UserTransaction</code> (aka JTA transaction), most likely the
 * cause of the <code>UserTransaction</code>'s
 * <code>javax.transaction.Status#STATUS_MARKED_ROLLBACK</code>
 * status.
 *
 * @author Mitch Thomas
 *
 * @see javax.transaction.UserTransaction#setRollbackOnly
 * @see javax.transaction.Transaction#registerSynchronization
 * @see javax.transaction.Status
 * @since 4.0
 */
public class FailedSynchronizationException extends CacheRuntimeException {
private static final long serialVersionUID = -6225053492344591496L;
  /**
   * Constructs an instance of
   * <code>FailedSynchronizationException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public FailedSynchronizationException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of
   * <code>FailedSynchronizationException</code> with the
   * specified detail message and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public FailedSynchronizationException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
