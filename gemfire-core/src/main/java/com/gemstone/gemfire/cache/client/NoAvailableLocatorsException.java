/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;


/**
 * An exception indicating that there are no active locators available to connect to.
 * @author dsmith
 * @since 5.7
 */
public class NoAvailableLocatorsException extends ServerConnectivityException {
  private static final long serialVersionUID = -8212446737778234890L;

  /**
   * Create a new instance of NoAvailableLocatorsException without a detail message or cause.
   */
  public NoAvailableLocatorsException() {
  }

  /**
   * Create a new instance of NoAvailableServersException with a detail message
   * @param message the detail message
   */
  public NoAvailableLocatorsException(String message) {
    super(message);
  }

  /**
   * Create a new instance of NoAvailableLocatorsException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public NoAvailableLocatorsException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of NoAvailableLocatorsException with a and cause
   * @param cause the cause
   */
  public NoAvailableLocatorsException(Throwable cause) {
    super(cause);
  }

}
