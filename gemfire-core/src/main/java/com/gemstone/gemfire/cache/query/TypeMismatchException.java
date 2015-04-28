/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if type consistency is violated while a query is being executed.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class TypeMismatchException extends QueryException {
private static final long serialVersionUID = 4205901708655503775L;
  
  /**
   * Construct an instance of TypeMismatchException
   * @param msg the error message
   */
  public TypeMismatchException(String msg) {
    super(msg);
  }
    
  /**
   * Construct an instance of TypeMismatchException
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public TypeMismatchException(String msg, Throwable cause) {
    super(msg, cause);
  }  
}
