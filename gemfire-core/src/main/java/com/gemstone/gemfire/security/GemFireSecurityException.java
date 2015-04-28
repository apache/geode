/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.security;

import com.gemstone.gemfire.GemFireException;

/**
 * The base class for all {@link com.gemstone.gemfire.security} related
 * exceptions.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class GemFireSecurityException extends GemFireException {
private static final long serialVersionUID = 3814254578203076926L;

  /**
   * Constructs instance of <code>SecurityException</code> with error message.
   * 
   * @param message
   *                the error message
   */
  public GemFireSecurityException(String message) {
    super(message);
  }

  /**
   * Constructs instance of <code>SecurityException</code> with error message
   * and cause.
   * 
   * @param message
   *                the error message
   * @param cause
   *                a <code>Throwable</code> that is a cause of this exception
   */
  public GemFireSecurityException(String message, Throwable cause) {
    super(message, cause);
  }

}
