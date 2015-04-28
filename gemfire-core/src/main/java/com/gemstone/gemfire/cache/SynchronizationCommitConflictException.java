/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown when a commit operation of a JTA enlisted cache transaction fails
 *
 * @author Mitch Thomas
 *
 * @see javax.transaction.UserTransaction#commit
 * @since 4.0
 */
public class SynchronizationCommitConflictException extends CacheRuntimeException {
private static final long serialVersionUID = 2619806460255259492L;
  /**
   * Constructs an instance of
   * <code>SynchronizationCommitConflictException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public SynchronizationCommitConflictException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of
   * <code>SynchronizationCommitConflictException</code> with the
   * specified detail message and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public SynchronizationCommitConflictException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
