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
 * Thrown when a GemFire distributed system has been terminated.
 * 
 * @since 6.0
 */

public class DistributedSystemDisconnectedException extends CancelException {

  private static final long serialVersionUID = -2484849299224086250L;

  public DistributedSystemDisconnectedException() {
    super();
  }

  public DistributedSystemDisconnectedException(String message, Throwable cause) {
    super(message, cause);
  }

  public DistributedSystemDisconnectedException(Throwable cause) {
    super(cause);
  }

  public DistributedSystemDisconnectedException(String s) {
    super(s);
  }

}