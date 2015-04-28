/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.cache.control.ResourceManager;
/**
 * Thrown when the query is executing and the critical heap percentage is met.
 * @see ResourceManager#setCriticalHeapPercentage(float)
 * @see ResourceManager#getCriticalHeapPercentage()
 * 
 * If critical heap percentage is set, the query monitor can be disabled
 * from sending out QueryExecutionLowMemoryExeceptions at the risk of
 * a query exhausting all memory.
 *
 * @author jhuynh
 * @since 7.0
 */
public class QueryExecutionLowMemoryException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>QueryExecutionLowMemoryException</code> without detail message.
   */
  public QueryExecutionLowMemoryException() {
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public QueryExecutionLowMemoryException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public QueryExecutionLowMemoryException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>QueryExecutionLowMemoryException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public QueryExecutionLowMemoryException(Throwable cause) {
    super(cause);
  }
}
