/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

/**
 * A <code>SystemIsRunningException</code> is thrown when an operation
 * is attempted that requires that the locator is stopped.
 * <p>
 * In some cases this exception may be thrown and the locator will
 * not be running. This will happen if the locator was not stopped
 * cleanly.
 * <p>As of GemFire 5.0 this exception should be named LocatorIsRunningException.
 */
public class SystemIsRunningException extends GemFireException {
private static final long serialVersionUID = 3516268055878767189L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemIsRunningException</code>.
   */
  public SystemIsRunningException() {
    super();
  }

  /**
   * Creates a new <code>SystemIsRunningException</code>.
   */
  public SystemIsRunningException(String message) {
    super(message);
  }
}
