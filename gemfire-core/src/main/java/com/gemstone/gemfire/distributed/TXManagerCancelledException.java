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
 * Thrown when a GemFire transaction manager has been terminated.
 * 
 * @since 6.0
 */

public class TXManagerCancelledException extends CancelException {

  private static final long serialVersionUID = 3902857360354568446L;

  public TXManagerCancelledException() {
    super();
  }

  public TXManagerCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public TXManagerCancelledException(Throwable cause) {
    super(cause);
  }

  public TXManagerCancelledException(String s) {
    super(s);
  }

}