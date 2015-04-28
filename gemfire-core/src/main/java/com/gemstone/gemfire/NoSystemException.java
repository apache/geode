/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * A <code>NoSystemException</code> is thrown when a
 * locator can not be found or connected to.
 * In most cases one of the following subclasses is used instead
 * of <code>NoSystemException</code>:
 * <ul>
 * <li> {@link UncreatedSystemException}
 * <li> {@link UnstartedSystemException}
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named NoLocatorException.
 */
public class NoSystemException extends GemFireException {
private static final long serialVersionUID = -101890149467219630L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>NoSystemException</code>.
   */
  public NoSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>NoSystemException</code> with the given message
   * and cause.
   */
  public NoSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
