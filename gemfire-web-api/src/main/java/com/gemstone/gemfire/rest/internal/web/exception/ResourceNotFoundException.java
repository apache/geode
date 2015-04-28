/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


package com.gemstone.gemfire.rest.internal.web.exception;

/**
 * Indicates that resource (key, value etc) does not found while trying to do REST operation.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */

public class ResourceNotFoundException extends RuntimeException {

  public ResourceNotFoundException() {
  }

  public ResourceNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }
  
  public ResourceNotFoundException(String message) {
    super(message);
  }

  public ResourceNotFoundException(Throwable cause) {
    super(cause);
  }
  
}
