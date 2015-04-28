/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;


/**
 * A callback that is allowed to veto a transaction. Only one TransactionWriter can exist 
 * per cache, and only one TransactionWriter will be fired in the 
 * entire distributed system for each transaction.
 *
 * This writer can be used to update a backend data source before the GemFire cache is updated during commit.
 * If the backend update fails, the implementer can throw a {@link TransactionWriterException} to veto the transaction.
 * @see CacheTransactionManager#setWriter
 * @since 6.5
 */

public interface TransactionWriter extends CacheCallback {
  
  /** Called before the transaction has finished committing, but after conflict checking. 
   * Provides an opportunity for implementors to cause transaction abort by throwing a
   * TransactionWriterException
   * 
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   * @throws TransactionWriterException in the event that the transaction should be rolled back
   */
  public void beforeCommit(TransactionEvent event) throws TransactionWriterException;

}
