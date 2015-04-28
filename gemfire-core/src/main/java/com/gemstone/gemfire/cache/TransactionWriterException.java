/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;



/**
 * Exception thrown by implementors of {@link TransactionWriter#beforeCommit} to 
 * signal that the current transaction should be aborted.
 * 
 * @see TransactionWriter#beforeCommit
 * @author gregp
 * @since 6.5
 *
 */
public class TransactionWriterException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -5557392877576634835L;

  public TransactionWriterException(String s) {
    super(s);
  }
  
  public TransactionWriterException(Throwable t) {
    super(t);
  }
}
