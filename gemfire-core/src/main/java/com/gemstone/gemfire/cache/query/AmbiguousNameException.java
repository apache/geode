/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if an attribute or method name in a query can be resolved to
 * more than one object in scope or if there is more than one maximally specific
 * overridden method in a class.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class AmbiguousNameException extends NameResolutionException {
private static final long serialVersionUID = 5635771575414148564L;
  
  /**
   * Constructs instance of AmbiguousNameException with error message
   * @param msg the error message
   */
  public AmbiguousNameException(String msg) {
    super(msg);
  }
    
  /**
   * Constructs instance of AmbiguousNameException with error message and cause
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public AmbiguousNameException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
