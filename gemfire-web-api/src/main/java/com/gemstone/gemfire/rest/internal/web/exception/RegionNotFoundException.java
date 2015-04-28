/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.exception;

/**
 * Indicates that Region does not found while trying to do some REST operation on that region.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */
@SuppressWarnings("unused")
public class RegionNotFoundException extends ResourceNotFoundException {

  public RegionNotFoundException() {          
  }

  public RegionNotFoundException(final String message) {
    super(message);
  }

  public RegionNotFoundException(final Throwable cause) {
    super(cause);
  }

  public RegionNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
