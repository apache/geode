/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * This is the superclass for all Exceptions that may be thrown
 * by a GemFire transaction.
 * @author sbawaska
 * @since 6.5
 */
public class TransactionException extends CacheException {

  private static final long serialVersionUID = -8400774340264221993L;

  public TransactionException() {
  }

  public TransactionException(String message) {
    super(message);
  }

  public TransactionException(Throwable cause) {
    super(cause);
  }
  
  public TransactionException(String message, Throwable cause) {
    super(message, cause);
  }
}
