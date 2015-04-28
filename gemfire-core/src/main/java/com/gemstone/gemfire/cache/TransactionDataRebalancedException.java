/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.cache.control.RebalanceOperation;

/**
 * Thrown when a {@link RebalanceOperation} occurs concurrently with a transaction.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * involves partitioned regions.
 * 
 * @author gregp
 * @since 6.6
 */
public class TransactionDataRebalancedException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public TransactionDataRebalancedException(String s) {
    super(s);
  }
}
