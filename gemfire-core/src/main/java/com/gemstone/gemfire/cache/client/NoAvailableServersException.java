/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;


/**
 * An exception indicating that there are no active servers available to connect to.
 * @author dsmith
 * @since 5.7
 */
public class NoAvailableServersException extends ServerConnectivityException {
  private static final long serialVersionUID = -8212446737778234890L;

  /**
   * Create a new instance of NoAvailableServersException without a detail message or cause.
   */
  public NoAvailableServersException() {
  }

  /**
   * Create a new instance of NoAvailableServersException with a detail message
   * @param message the detail message
   */
  public NoAvailableServersException(String message) {
    super(message);
  }

  /**
   * Create a new instance of NoAvailableServersException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public NoAvailableServersException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of NoAvailableServersException with a and cause
   * @param cause the cause
   */
  public NoAvailableServersException(Throwable cause) {
    super(cause);
  }

}
