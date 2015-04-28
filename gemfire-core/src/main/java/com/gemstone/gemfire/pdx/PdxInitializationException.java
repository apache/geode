/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the PDX system could not be successfully initialized.
 * The cause will give the detailed reason why initialization failed.
 * @author darrel
 * @since 6.6
 *
 */
public class PdxInitializationException extends GemFireException {

  private static final long serialVersionUID = 5098737377658808834L;

  /**
   * Construct a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxInitializationException(String message) {
    super(message);
  }

  /**
   * Construct a new exception with the given message and cause
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public PdxInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
