/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown if a <code>netSearch</code> times out without getting a response back from a cache,
 * or when attempting to acquire a distributed lock.
 *
 * @author Eric Zoerner
 *
 *
 * @see LoaderHelper#netSearch
 * @see com.gemstone.gemfire.cache.Region#invalidateRegion()
 * @see com.gemstone.gemfire.cache.Region#destroyRegion()
 * @see Region#createSubregion
 * @see com.gemstone.gemfire.cache.Region#get(Object)
 * @see com.gemstone.gemfire.cache.Region#put(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#create(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#invalidate(Object)
 * @see com.gemstone.gemfire.cache.Region#destroy(Object)
 * @see com.gemstone.gemfire.distributed.DistributedLockService
 * @since 3.0
 */
public class TimeoutException extends OperationAbortedException {
private static final long serialVersionUID = -6260761691185737442L;
  
  /**
   * Creates a new instance of <code>TimeoutException</code> without detail message.
   */
  public TimeoutException() {
  }
  
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified detail message.
   * @param msg the detail message
   */
  public TimeoutException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public TimeoutException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>TimeoutException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public TimeoutException(Throwable cause) {
    super(cause);
  }
}
