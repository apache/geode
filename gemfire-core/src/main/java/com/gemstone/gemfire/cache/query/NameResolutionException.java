/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if an attribute or method name in a query cannot be resolved.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class NameResolutionException extends QueryException {
private static final long serialVersionUID = -7409771357534316562L;
  
  /**
   * Constructs a NameResolutionException
   * @param msg the error message
   */
  public NameResolutionException(String msg) {
    super(msg);
  }
    
  /**
   * Constructs a NameResolutionException
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public NameResolutionException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  
}
