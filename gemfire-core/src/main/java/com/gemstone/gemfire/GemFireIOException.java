/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * A <code>GemFireIOException</code> is thrown when a 
 * GemFire operation failure is caused by an <code>IOException</code>.
 *
 * @author David Whitlock
 *
 */
public class GemFireIOException extends GemFireException {
private static final long serialVersionUID = 5694009444435264497L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireIOException</code>.
   */
  public GemFireIOException(String message, Throwable cause) {
    super(message, cause);
  }
  public GemFireIOException(String message) {
    super(message);
  }
}
