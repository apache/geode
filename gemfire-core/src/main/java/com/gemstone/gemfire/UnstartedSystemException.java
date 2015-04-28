/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>UnstartedSystemException</code> is thrown when the specified
 * locator exists but is not running or could not be connected to.
 * <p>
 * The most likely reasons for this are:
 * <ul>
 * <li> The locator has not completely started.
 * <li> The locator is stopping.
 * <li> The locator died or was killed.
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named UnstartedLocatorException.
 */
public class UnstartedSystemException extends NoSystemException {
private static final long serialVersionUID = -4285897556527521788L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>UnstartedSystemException</code>.
   */
  public UnstartedSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>UnstartedSystemException</code> with the given message
   * and cause.
   */
  public UnstartedSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
