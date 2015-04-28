/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * This Exception is thrown in presence of node failures, when GemFire cannot
 * know with certainty about the outcome of the transaction.
 * @author sbawaska
 * @since 6.5
 */
public class TransactionInDoubtException extends TransactionException {
  private static final long serialVersionUID = 4895453685211922512L;

  public TransactionInDoubtException() {
  }

  public TransactionInDoubtException(String message) {
    super(message);
  }

  public TransactionInDoubtException(Throwable cause) {
    super(cause);
  }

  public TransactionInDoubtException(String message, Throwable cause) {
    super(message, cause);
  }
}
