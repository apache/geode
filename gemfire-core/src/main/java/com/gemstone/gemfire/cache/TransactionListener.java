/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * <p>A listener that can be implemented to handle transaction related
 * events.  The methods on <code>TransactionListener</code> are
 * invoked synchronously after the operation, commit or rollback,
 * completes.  The transaction that causes the listener to be called
 * will no longer exist at the time the listener code executes.  The
 * thread that performed the transaction operation will not see that
 * operation complete until the listener method completes its
 * execution. 
 *
 * <p>Multiple transactions, on the same cache, can cause concurrent
 * invocation of <code>TransactionListener</code> methods.  Any
 * exceptions thrown by the listener are caught and logged.
 *
 * <p>Rollback and failed commit operations are local.
 *
 * @author Darrel Schneider
 *
 * @see CacheTransactionManager#setListener
 * @see CacheTransactionManager#getListener
 * @since 4.0
 */

public interface TransactionListener extends CacheCallback {
  
  /** Called after a successful commit of a transaction.
   * 
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   */
  public void afterCommit(TransactionEvent event);

  /** Called after an unsuccessful commit operation.
   * 
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   */
  public void afterFailedCommit(TransactionEvent event);

  /** Called after an explicit rollback of a transaction.
   * 
   * @param event the TransactionEvent
   * @see CacheTransactionManager#rollback
   * @see CacheTransactionManager#commit
   */
  public void afterRollback(TransactionEvent event);
}
