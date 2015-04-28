/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * An exception thrown by a <code>Gateway</code>.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
public class GatewayException extends OperationAbortedException {
private static final long serialVersionUID = 8090143153569084886L;

  /**
   * Constructor.
   * Creates a new instance of <code>GatewayException</code>.
   */
  public GatewayException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public GatewayException(String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public GatewayException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public GatewayException(Throwable cause) {
    super(cause);
  }
}
