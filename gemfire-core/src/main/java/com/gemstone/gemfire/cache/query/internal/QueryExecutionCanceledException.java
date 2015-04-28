/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.cache.control.ResourceManager;
/**
 * Internal exception thrown when a query has been canceled and QueryMonitor.isQueryExecutionCanceled() is called
 * Due to various threads using the method, access to the query object may not be available for certain threads
 * This exception is generically used and caught by DefaultQuery, which will then throw the appropriate exception

 * @author jhuynh
 * @since 7.0
 */
public class QueryExecutionCanceledException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>QueryExecutionCanceledException</code> without detail message.
   */
  public QueryExecutionCanceledException() {
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public QueryExecutionCanceledException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionCanceledException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionCanceledException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public QueryExecutionCanceledException(Throwable cause) {
    super(cause);
  }
}
