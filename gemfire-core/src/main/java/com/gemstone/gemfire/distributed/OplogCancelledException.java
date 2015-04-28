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
 * Thrown when a GemFire gateway has been terminated.
 * 
 * @since 6.0
 */

public class OplogCancelledException extends CancelException {

  private static final long serialVersionUID = 106566926222526806L;

  public OplogCancelledException() {
    super();
  }

  public OplogCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public OplogCancelledException(Throwable cause) {
    super(cause);
  }

  public OplogCancelledException(String s) {
    super(s);
  }

}