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

import com.gemstone.gemfire.SerializationException;

/**
 * Thrown if a problem occurred during serialization
 * or deserialization of a PDX. In most cases consult the cause for
 * a description of the problem.
 * 
 * @author darrel
 * @since 6.6
 */
public class PdxSerializationException extends SerializationException {

  private static final long serialVersionUID = -3843814927034345635L;

  /**
   * Create the exception with the given message.
   * @param message the message of the new exception
   */
  public PdxSerializationException(String message) {
    super(message);
  }

  /**
   * Create the exception with the given message and cause.
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public PdxSerializationException(String message, Throwable cause) {
    super(message, cause);
  }

}
