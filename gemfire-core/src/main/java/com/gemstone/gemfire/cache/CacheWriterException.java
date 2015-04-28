/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * An exception thrown by a <code>CacheWriter</code> method. This exception
 * is propagated back to the caller that initiated modification of the
 * cache, even if the caller is not in the same cache VM.
 *
 * @author Eric Zoerner
 *
 * @see CacheWriter
 * @see com.gemstone.gemfire.cache.Region#put(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#destroy(Object)
 * @see com.gemstone.gemfire.cache.Region#create(Object, Object)
 * @see com.gemstone.gemfire.cache.Region#destroyRegion()
 * @see com.gemstone.gemfire.cache.Region#get(Object)
 * @since 3.0
 */
public class CacheWriterException extends OperationAbortedException {
private static final long serialVersionUID = -2872212342970454458L;
  
  /**
   * Creates a new instance of <code>CacheWriterException</code>.
   */
  public CacheWriterException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheWriterException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheWriterException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheWriterException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheWriterException(Throwable cause) {
    super(cause);
  }
}
