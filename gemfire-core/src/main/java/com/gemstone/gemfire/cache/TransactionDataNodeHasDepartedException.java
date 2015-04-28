/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Thrown when the transactional data host has shutdown or no longer has the data
 * being modified by the transaction.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * @author gregp
 * @since 6.5
 */
public class TransactionDataNodeHasDepartedException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public TransactionDataNodeHasDepartedException(String s) {
    super(s);
  }
  
  public TransactionDataNodeHasDepartedException(Throwable e) {
    super(e);
  }
}
