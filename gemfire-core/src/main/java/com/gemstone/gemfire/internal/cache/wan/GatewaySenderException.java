/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.cache.OperationAbortedException;

public class GatewaySenderException extends OperationAbortedException {
private static final long serialVersionUID = 8090143153569084886L;

  /**
   * Constructor.
   * Creates a new instance of <code>GatewaySenderException</code>.
   */
  public GatewaySenderException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewaySenderException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public GatewaySenderException(String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewaySenderException</code> with the
   * specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public GatewaySenderException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewaySenderException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public GatewaySenderException(Throwable cause) {
    super(cause);
  }
}
