/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Exception thrown from Connection Pool.
 * 
 * @author rreja
 */
public class PoolException extends GemFireCheckedException {
private static final long serialVersionUID = -6178632158204356727L;

  //public Exception excep;

  /** Creates a new instance of PoolException */
  public PoolException() {
    super();
  }

  /**
   * @param message
   */
  public PoolException(String message) {
    super(message);
  }

  /**
   * Single Argument constructor to construct a new exception with the specified
   * detail message. Calls Exception class constructor.
   * 
   * @param message The detail message. The detail message is saved for later
   *          retrieval.
   */
  public PoolException(String message, Exception ex) {
    super(message,ex);
    //this.excep = ex;
  }

}
