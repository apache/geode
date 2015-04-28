/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if an exception is thrown when a method is invoked during query execution.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class QueryInvocationTargetException extends QueryException {
private static final long serialVersionUID = 2978208305701582906L;
  
  /**
   * Construct an instance of QueryInvalidException
   * @param cause a Throwable cause of this exception
   */
  public QueryInvocationTargetException(Throwable cause) {
    super(cause);
  }
  
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   */
  public QueryInvocationTargetException(String msg) {
    super(msg);
  }
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public QueryInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }  
}
