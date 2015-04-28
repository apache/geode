/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;


/**
 * Indicates that the connection pool is at its maximum size and
 * all connections are in use.
 * @author dsmith
 * @since 5.7
 */
public class AllConnectionsInUseException extends ServerConnectivityException {

  private static final long serialVersionUID = 7304243507881787071L;

  /**
   * Create a new instance of AllConnectionsInUseException without a detail message or cause.
   */
  public AllConnectionsInUseException() {
  }

  /**
   * Create a new instance of AllConnectionsInUseException with a detail message
   * @param message the detail message
   */
  public AllConnectionsInUseException(String message) {
    super(message);
  }
  
  /**
   * Create a new instance of AllConnectionsInUseException with a cause
   * @param cause the cause
   */
  public AllConnectionsInUseException(Throwable cause) {
    super(cause);
  }
  
  /**
   * Create a new instance of AllConnectionsInUseException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public AllConnectionsInUseException(String message, Throwable cause) {
    super(message, cause);
  }

}
