/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import com.gemstone.gemfire.GemFireIOException;

/**
 * An exception indicating that a serialization or deserialization failed.
 * @author darrel
 * @since 5.7
 */
public class SerializationException extends GemFireIOException {
private static final long serialVersionUID = 7783018024920098997L;
  /**
   * 
   * Create a new instance of SerializationException with a detail message
   * @param message the detail message
   */
  public SerializationException(String message) {
    super(message);
  }

  /**
   * Create a new instance of SerializationException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
