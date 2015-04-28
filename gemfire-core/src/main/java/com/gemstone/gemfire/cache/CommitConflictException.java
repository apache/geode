/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown when a commit fails due to a write conflict.
 *
 * @author Darrel Schneider
 *
 * @see CacheTransactionManager#commit
 * @since 4.0
 */
public class CommitConflictException extends TransactionException {
  private static final long serialVersionUID = -1491184174802596675L;

  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CommitConflictException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CommitConflictException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CommitConflictException</code> with the specified cause.
   * @param cause the causal Throwable
   * @since 6.5
   */
  public CommitConflictException(Throwable cause) {
    super(cause);
  }
  
}
