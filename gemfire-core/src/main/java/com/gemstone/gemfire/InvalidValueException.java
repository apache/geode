/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>InvalidValueException</code> is thrown when an attempt is
 * made to set a configuration attribute to an invalid value is made.
 * Values are considered invalid when they are
 * not compatible with the attribute's type.
 */
public class InvalidValueException extends GemFireException {
private static final long serialVersionUID = 6186767885369527709L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>InvalidValueException</code>.
   */
  public InvalidValueException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>InvalidValueException</code>.
   */
  public InvalidValueException(String message, Throwable ex) {
    super(message, ex);
  }
}
