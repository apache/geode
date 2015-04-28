/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Indicates that the operation that
 * would have otherwise affected the cache has been aborted.
 * Only subclasses are instantiated.
 *
 * @author Eric Zoerner
 *
 *
 * @since 3.0
 */
public abstract class OperationAbortedException extends CacheRuntimeException {
  
  /**
   * Creates a new instance of <code>OperationAbortedException</code> without detail message.
   */
  public OperationAbortedException() {
  }
  
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified detail message.
   * @param msg the detail message
   */
  public OperationAbortedException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public OperationAbortedException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>OperationAbortedException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public OperationAbortedException(Throwable cause) {
    super(cause);
  }
}
