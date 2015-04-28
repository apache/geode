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
 * Thrown when a configuration that is now allowed by PDX is detected.
 * @author darrel
 * @since 6.6
 *
 */
public class PdxConfigurationException extends GemFireException {

  private static final long serialVersionUID = -2329989020829052537L;

  /**
   * Constructs a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxConfigurationException(String message) {
    super(message);
  }
}
