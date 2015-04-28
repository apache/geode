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
 * Indicates that this client cannot contact any queue servers and
 * therefore cannot perform operations that require a queue, such as
 * registering interest.
 * @author dsmith
 * @since 5.7
 */
public class NoQueueServersAvailableException extends ServerConnectivityException {

  private static final long serialVersionUID = 8484086019155762365L;

  /**
   * Create a new instance of NoPrimaryAvailableException without a detail message or cause.
   */
  public NoQueueServersAvailableException() {
  }

  /**
   * 
   * Create a new instance of NoPrimaryAvailableException with a detail message
   * @param message the detail message
   */
  public NoQueueServersAvailableException(String message) {
    super(message);
  }

  /**
   * Create a new instance of NoPrimaryAvailableException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public NoQueueServersAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of NoPrimaryAvailableException with a cause
   * @param cause the cause
   */
  public NoQueueServersAvailableException(Throwable cause) {
    super(cause);
  }

}
