/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * Indicates a failure to copy an object.
 *
 * @author Darrel Schneider
 *
 *
 * @see CopyHelper#copy
 * @since 4.0
 */
public class CopyException extends GemFireException {
private static final long serialVersionUID = -1143711608610323585L;
  
  /**
   * Constructs a new <code>CopyException</code>.
   */
  public CopyException() {
    super();
  }
  
  /**
   * Constructs a new <code>CopyException</code> with a message string.
   *
   * @param msg a message string
   */
  public CopyException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs a new <code>CopyException</code> with a message string
   * and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   */
  public CopyException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs a new <code>CopyException</code> with a cause.
   *
   * @param cause a causal Throwable
   */
  public CopyException(Throwable cause) {
    super(cause);
  }
}

