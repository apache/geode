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
 * Thrown when a GemFire pool has been cancelled.
 * 
 * @since 6.0
 */

public class PoolCancelledException extends CancelException {

  private static final long serialVersionUID = -4562742255812266767L;

  public PoolCancelledException() {
    super();
  }

  public PoolCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public PoolCancelledException(Throwable cause) {
    super(cause);
  }

  public PoolCancelledException(String s) {
    super(s);
  }

}