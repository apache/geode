/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>UncreatedSystemException</code> is thrown when the specified
 * locator's directory or configuration file can not be found.
 * <p>
 * The most likely reasons for this are:
 * <ul>
 * <li> The wrong locator directory was given.
 * <li> The locator was deleted or never created.
 * </ul>
 * <p>As of GemFire 5.0 this exception should be named UncreatedLocatorException.
 */
public class UncreatedSystemException extends NoSystemException {
private static final long serialVersionUID = 5424354567878425435L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>UncreatedSystemException</code>.
   */
  public UncreatedSystemException(String message) {
    super(message);
  }
  /**
   * Creates a new <code>UncreatedSystemException</code> with the given message
   * and cause.
   */
  public UncreatedSystemException(String message, Throwable cause) {
      super(message, cause);
  }
}
