/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.security;

/**
 * Thrown if authentication of this client/peer fails.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class AuthenticationFailedException extends GemFireSecurityException {
private static final long serialVersionUID = -8202866472279088879L;

  // TODO Derive from SecurityException
  /**
   * Constructs instance of <code>AuthenticationFailedException</code> with
   * error message.
   * 
   * @param message
   *                the error message
   */
  public AuthenticationFailedException(String message) {
    super(message);
  }

  /**
   * Constructs instance of <code>AuthenticationFailedException</code> with
   * error message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public AuthenticationFailedException(String message, Throwable cause) {
    super(message, cause);
  }

}
