/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheRuntimeException;
/**
 * Thrown when the query execution takes more than the specified max time.
 * The Max query execution time is set using the system  variable 
 * gemfire.Cache.MAX_QUERY_EXECUTION_TIME. 
 *
 * @author agingade
 * @since 6.0
 */
public class QueryExecutionTimeoutException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>QueryExecutionTimeoutException</code> without detail message.
   */
  public QueryExecutionTimeoutException() {
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public QueryExecutionTimeoutException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionTimeoutException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionTimeoutException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public QueryExecutionTimeoutException(Throwable cause) {
    super(cause);
  }
}
