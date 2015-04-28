/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * An <code>InternalGemFireException</code> is thrown when
 * a low level, internal, operation fails due to no fault of
 * the user. The message often contains an operating system
 * error code.
 *
 * @author David Whitlock
 *
 */
public class InternalGemFireException extends GemFireException {
private static final long serialVersionUID = -6912843691545178619L;

  //////////////////////  Constructors  //////////////////////

  public InternalGemFireException() {
    super();
  }

  public InternalGemFireException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>InternalGemFireException</code>.
   */
  public InternalGemFireException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>InternalGemFireException</code> that was
   * caused by a given exception
   */
  public InternalGemFireException(String message, Throwable thr) {
    super(message, thr);
  }
}
