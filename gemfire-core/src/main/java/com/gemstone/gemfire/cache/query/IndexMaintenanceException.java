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
 * Thrown if an error occurs while updating query indexes during
 * region modification.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public class IndexMaintenanceException extends CacheRuntimeException {
private static final long serialVersionUID = 3326023943226474039L;
  
  /**
   * Creates a new instance of <code>IndexMaintenanceException</code> without detail message.
   */
  public IndexMaintenanceException() {
  }
  
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public IndexMaintenanceException(String msg) {
    super(msg);
  }
  
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public IndexMaintenanceException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public IndexMaintenanceException(Throwable cause) {
    super(cause);
  }
  
  
}
