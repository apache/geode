/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.util.concurrent.Future;
import com.gemstone.gemfire.CancelException;

/**
 * Thrown when a {@link Future} has been cancelled.
 * 
 * @since 6.0
 */

public class FutureCancelledException extends CancelException {
  private static final long serialVersionUID = -4599338440381989844L;

  public FutureCancelledException() {
    super();
  }

  public FutureCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public FutureCancelledException(Throwable cause) {
    super(cause);
  }

  public FutureCancelledException(String s) {
    super(s);
  }

}
