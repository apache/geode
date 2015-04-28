/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.GemFireException;

/**
 * A generic exception indicating that a failure has happened while communicating
 * with a gemfire server. Subclasses of this exception provide more detail
 * on specific failures.
 * @author dsmith
 * @since 5.7
 */
public class ServerConnectivityException extends GemFireException {
private static final long serialVersionUID = -5205644901262051330L;

  /**
   * Create a new instance of ServerConnectivityException without a detail message or cause.
   */
  public ServerConnectivityException() {
  }

  /**
   * 
   * Create a new instance of ServerConnectivityException with a detail message
   * @param message the detail message
   */
  public ServerConnectivityException(String message) {
    super(message);
  }

  /**
   * Create a new instance of ServerConnectivityException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public ServerConnectivityException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of ServerConnectivityException with a cause
   * @param cause the cause
   */
  public ServerConnectivityException(Throwable cause) {
    super(cause);
  }

}
