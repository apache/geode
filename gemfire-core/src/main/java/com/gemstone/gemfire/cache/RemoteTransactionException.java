/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that an unexpected runtime exception occurred
 * during a cache operation on the transactional data host.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * @author gregp
 * @since 6.5
 * @deprecated as of 6.6 exceptions from a remote node are no longer wrapped in this exception.  Instead of this, {@link TransactionDataNodeHasDepartedException} is thrown.
 */
public class RemoteTransactionException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public RemoteTransactionException(String s) {
    super(s);
  }
  
  public RemoteTransactionException(Exception e) {
    super(e);
  }
}
