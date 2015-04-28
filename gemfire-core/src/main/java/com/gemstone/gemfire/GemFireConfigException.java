/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * A <code>GemFireConfigException</code> is used for failures
 * while processing a GemFire configuration XML file.
 */
public class GemFireConfigException extends GemFireException {
private static final long serialVersionUID = 7791789785331120991L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireConfigException</code>.
   */
  public GemFireConfigException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>GemFireConfigException</code>.
   */
  public GemFireConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
