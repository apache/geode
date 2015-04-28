/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;


/**
 * Thrown when an attribute or method name could not be resolved during query
 * execution because no matching method or field could be found.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class NameNotFoundException extends NameResolutionException {
private static final long serialVersionUID = 4827972941932684358L;
  /**
   * Constructs instance of ObjectNameNotFoundException with error message
   * @param message the error message
   */
  public NameNotFoundException(String message) {
    super(message);
  }
  
  /**
   * Constructs instance of ObjectNameNotFoundException with error message and cause
   * @param message the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public NameNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
  
}
