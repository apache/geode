/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell;

/**
 * RuntimeException to wrap JMX Connection Error/Exception.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class JMXConnectionException extends RuntimeException {
  private static final long serialVersionUID = 3872374016604940917L;

  public static final int OTHER                       = 1;
  public static final int MANAGER_NOT_FOUND_EXCEPTION = 2;
  public static final int CONNECTION_EXCEPTION        = 3;
  
  private int exceptionType;

  public JMXConnectionException(String message, Throwable cause, int exceptionType) {
    super(message, cause);
    this.exceptionType = exceptionType;
  }

  public JMXConnectionException(String message, int exceptionType) {
    super(message);
    this.exceptionType = exceptionType;
  }

  public JMXConnectionException(Throwable cause, int exceptionType) {
    super(cause);
    this.exceptionType = exceptionType;
  }
  
  public JMXConnectionException(int exceptionType) {
    this.exceptionType = exceptionType;
  }

  public int getExceptionType() {
    return exceptionType;
  }
}
