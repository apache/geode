/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that an attempt was made to transactionally modify multiple keys that
 * are not colocated on the same data host.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * <p>Note: a rebalance can cause this exception to be thrown for data that
 * is usually colocated. This is because data can be moved from one node to another
 * during the time between the original transactional operations and the commit. 
 *
 * @author gregp
 * @since 6.5
 */
public class TransactionDataNotColocatedException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public TransactionDataNotColocatedException(String s) {
    super(s);
  }

  public TransactionDataNotColocatedException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
