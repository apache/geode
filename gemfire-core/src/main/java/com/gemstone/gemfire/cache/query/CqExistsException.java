/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * Thrown if a CQ by this name already exists on this client 
 * @author Anil 
 * @since 5.5
 */

public class CqExistsException extends QueryException {
private static final long serialVersionUID = -4805225282677926623L;
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public CqExistsException(String msg, Throwable cause) {
    super(msg, cause);
  }  
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   */
  public CqExistsException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param cause a Throwable cause of this exception
   */
  public CqExistsException(Throwable cause) {
    super(cause);
  }  
}
