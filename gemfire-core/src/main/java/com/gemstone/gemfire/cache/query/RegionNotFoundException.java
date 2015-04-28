/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if a region referenced by name in a query cannot be found.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class RegionNotFoundException extends NameResolutionException {
private static final long serialVersionUID = 592495934010222373L;
  
  /**
   * Construct an instance of RegionNotFoundException
   * @param msg the error message
   */
  public RegionNotFoundException(String msg) {
    super(msg);
  }
    
  /**
   * Constructs an instance of RegionNotFoundException
   * @param msg the error message
   * @param cause a Throwable that is a cause of this exception
   */
  public RegionNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
