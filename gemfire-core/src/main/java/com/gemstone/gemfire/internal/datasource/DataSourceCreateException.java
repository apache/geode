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
 * Exception thrown from DataSource factory.
 * 
 * @author tnegi
 */
public class DataSourceCreateException extends GemFireCheckedException  {
private static final long serialVersionUID = 8759147832954825309L;

  public Exception excep;

  /** Creates a new instance of CreateConnectionException */
  public DataSourceCreateException() {
    super();
  }

  /**
   * @param message
   */
  public DataSourceCreateException(String message) {
    super(message);
  }

  /**
   * Single Argument constructor to construct a new exception with the specified
   * detail message. Calls Exception class constructor.
   * 
   * @param message The detail message. The detail message is saved for later
   *          retrieval.
   */
  public DataSourceCreateException(String message, Exception ex) {
    super(message);
    this.excep = ex;
  }

  /**
   * @return ???
   */
  @Override
  public StackTraceElement[] getStackTrace() {
    return excep.getStackTrace();
  }
}
