/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown from the {@link CacheLoader#load} method indicating that an error
 * occurred when a CacheLoader was attempting to load a value. This
 * exception is propagated back to the caller of <code>Region.get</code>.
 *
 * @author Eric Zoerner
 *
 *
 * @see com.gemstone.gemfire.cache.Region#get(Object)
 * @see CacheLoader#load
 * @since 3.0
 */
public class CacheLoaderException extends OperationAbortedException {
private static final long serialVersionUID = -3383072059406642140L;
  
  /**
   * Creates a new instance of <code>CacheLoaderException</code>.
   */
  public CacheLoaderException() {
  }
  
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified detail message.
   * @param msg the detail message
   */
  public CacheLoaderException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public CacheLoaderException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>CacheLoaderException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public CacheLoaderException(Throwable cause) {
    super(cause);
  }
}
