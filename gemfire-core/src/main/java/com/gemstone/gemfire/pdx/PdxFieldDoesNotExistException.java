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
 * Thrown when a PDX field does not exist and the current operation requires its existence.
 * PDX fields exist after they are written by one of the writeXXX methods on {@link PdxWriter}.
 * @author darrel
 * @since 6.6
 *
 */
public class PdxFieldDoesNotExistException extends GemFireException {

  private static final long serialVersionUID = 1617023951410281507L;

  /**
   * Constructs a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxFieldDoesNotExistException(String message) {
    super(message);
  }
}
