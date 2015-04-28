/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheRuntimeException;

/**
 * Thrown if the CqQuery on which the operation performed is closed.
 * 
 * @author Anil 
 * @since 5.5
 */


public class CqClosedException extends CacheRuntimeException {
private static final long serialVersionUID = -3793993436374495840L;
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public CqClosedException(String msg, Throwable cause) {
    super(msg, cause);
  }  
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   */
  public CqClosedException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param cause a Throwable cause of this exception
   */
  public CqClosedException(Throwable cause) {
    super(cause);
  }    
}
