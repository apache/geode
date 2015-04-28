/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.CancelException;

/**
 * Thrown when a GemFire oplog has been terminated.
 * 
 * @since 6.0
 */

public class GatewayCancelledException extends CancelException {
  private static final long serialVersionUID = -1444310105860938512L;

  public GatewayCancelledException() {
    super();
  }

  public GatewayCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public GatewayCancelledException(Throwable cause) {
    super(cause);
  }

  public GatewayCancelledException(String s) {
    super(s);
  }

}