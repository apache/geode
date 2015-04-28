/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.cache.OperationAbortedException;

/**
 * Exception observed during GatewayReceiver operations.
 * 
 * @author kbachhav
 * @since 8.1
 */
public class GatewayReceiverException extends OperationAbortedException {
  private static final long serialVersionUID = 7079321411869820364L;

  /**
   * Constructor.
   * Creates a new instance of <code>GatewayReceiverException</code>.
   */
  public GatewayReceiverException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayReceiverException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public GatewayReceiverException(String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayReceiverException</code> with the
   * specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public GatewayReceiverException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewayReceiverException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public GatewayReceiverException(Throwable cause) {
    super(cause);
  }
}
