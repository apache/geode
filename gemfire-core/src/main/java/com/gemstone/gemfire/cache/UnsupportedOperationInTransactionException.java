/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that an attempt was mode to invoke an operation that is not
 * allowed in a transaction.
 *
 * @author gregp
 * @since 6.5
 */
public class UnsupportedOperationInTransactionException extends
    UnsupportedOperationException {

  public UnsupportedOperationInTransactionException(String s) {
    super(s);
  }
  
  public UnsupportedOperationInTransactionException() {
    super();
  }
  
}
