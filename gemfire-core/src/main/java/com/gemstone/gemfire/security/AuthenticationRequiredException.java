/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.security;

/**
 * Thrown if the distributed system is in secure mode and this client/peer has
 * not set the security credentials.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class AuthenticationRequiredException extends GemFireSecurityException {
private static final long serialVersionUID = 4675976651103154919L;

  /**
   * Constructs instance of <code>NotAuthenticatedException</code> with error
   * message.
   * 
   * @param message
   *                the error message
   */
  public AuthenticationRequiredException(String message) {
    super(message);
  }

  /**
   * Constructs instance of <code>NotAuthenticatedException</code> with error
   * message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public AuthenticationRequiredException(String message, Throwable cause) {
    super(message, cause);
  }

}
