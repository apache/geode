/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CreateConnectionException.java
 *
 * Created on February 18, 2005, 3:34 PM
 */
package com.gemstone.gemfire.internal.datasource;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Exception thrown from the connection provider.
 * 
 * @author tnegi
 */
public class ConnectionProviderException extends GemFireCheckedException  {
private static final long serialVersionUID = -7406652144153958227L;

  public Exception excep;

  /** Creates a new instance of CreateConnectionException */
  public ConnectionProviderException() {
    super();
  }

  /**
   * @param message
   */
  public ConnectionProviderException(String message) {
    super(message);
  }

  /**
   * Single Argument constructor to construct a new exception with the specified
   * detail message. Calls Exception class constructor.
   * 
   * @param message The detail message. The detail message is saved for later
   *          retrieval.
   */
  public ConnectionProviderException(String message, Exception ex) {
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
