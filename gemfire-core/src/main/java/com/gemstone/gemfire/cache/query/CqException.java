/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * Thrown during continuous query creation, execution time.
 * 
 * @author Anil 
 * @since 5.5
 */


public class CqException extends QueryException {
private static final long serialVersionUID = -5905461592471139171L;
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public CqException(String msg, Throwable cause) {
    super(msg, cause);
  }  
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   */
  public CqException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param cause a Throwable cause of this exception
   */
  public CqException(Throwable cause) {
    super(cause);
  }  
  
  
}
