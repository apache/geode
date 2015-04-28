/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.security;

import java.security.Principal;

/**
 * Thrown when a client/peer is unauthorized to perform a requested operation.
 * 
 * @author Neeraj Kumar
 * @since 5.5
 */
public class NotAuthorizedException extends GemFireSecurityException {
private static final long serialVersionUID = 419215768216387745L;
  private Principal principal = null;
  /**
   * Constructs instance of <code>NotAuthorizedException</code> with error
   * message.
   * 
   * @param message
   *                the error message
   */
  public NotAuthorizedException(String message) {
    super(message);
  }

  public NotAuthorizedException(String message, Principal ppl) {
    super(message);
    this.principal = ppl;
  }
  
  public Principal getPrincipal() {
    return this.principal;
  }
  /**
   * Constructs instance of <code>NotAuthorizedException</code> with error
   * message and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public NotAuthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

}
