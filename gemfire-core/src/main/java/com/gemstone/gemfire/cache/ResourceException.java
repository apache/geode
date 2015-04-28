/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * A Generic exception to indicate that a resource exception has occurred.
 * This class is abstract so that only subclasses can be instantiated.
 * 
 * @author sbawaska
 * @since 6.0
 */
public abstract class ResourceException extends CacheRuntimeException {
  /**
   * Creates a new instance of <code>ResourceException</code> without detail message.
   */
  public ResourceException() {
  }
  
  
  /**
   * Constructs an instance of <code>ResourceException</code> with the specified detail message.
   * @param msg the detail message
   */
  public ResourceException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>ResourceException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public ResourceException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>ResourceException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public ResourceException(Throwable cause) {
    super(cause);
  }

}
