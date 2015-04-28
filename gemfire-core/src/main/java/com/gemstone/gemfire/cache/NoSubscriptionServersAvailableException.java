/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.cache.client.ServerConnectivityException;

/**
 * Indicates that this client cannot contact any servers and
 * therefore cannot perform operations that require subscriptions, such as
 * registering interest.
 * @author dsmith
 * @since 5.7
 */
public class NoSubscriptionServersAvailableException extends ServerConnectivityException {

  private static final long serialVersionUID = 8484086019155762365L;

  /**
   * Create a new instance of NoSubscriptionServersAvailableException without a detail message or cause.
   */
  public NoSubscriptionServersAvailableException() {
  }

  /**
   * 
   * Create a new instance of NoSubscriptionServersAvailableException with a detail message
   * @param message the detail message
   */
  public NoSubscriptionServersAvailableException(String message) {
    super(message);
  }

  /**
   * Create a new instance of NoSubscriptionServersAvailableException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public NoSubscriptionServersAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of NoSubscriptionServersAvailableException with a cause
   * @param cause the cause
   */
  public NoSubscriptionServersAvailableException(Throwable cause) {
    super(cause);
  }

}
