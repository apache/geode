/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.exception;


import com.gemstone.gemfire.GemFireException;

/**
 * Indicates that error encountered while converting Non-Pdx type values/data into RESTful format, i.e JSON
 * <p/>
 * @author Nilkanth Patel
 * @since 8.1
 */

public class DataTypeNotSupportedException extends GemFireException {

  /**
   * Create the exception with the given message.
   * @param message the message of the new exception
   */
  public DataTypeNotSupportedException(String message) {
    super(message);
  }

  /**
   * Create the exception with the given message and cause.
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public DataTypeNotSupportedException(String message, Throwable cause) {
    super(message, cause);
  }
  
}

