/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception will be thrown, when  {link @JSONFormatter} is unable to parse the 
 * JSON document or {link @PdxInstance}.
 */

public class JSONFormatterException extends GemFireException{

  private static final long serialVersionUID = 1L;

  /**
   * Create the exception with the given message.
   * @param message the message of the new exception
   */
  public JSONFormatterException(String message) {
    super(message);
  }

  /**
   * Create the exception with the given message and cause.
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public JSONFormatterException(String message, Throwable cause) {
    super(message, cause);
  }

}
