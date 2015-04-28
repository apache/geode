/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.pooling;

import com.gemstone.gemfire.GemFireException;

/**
 * Indicates that the current connection has already been destroyed.
 * This exception should not propagate all the way back to the 
 * user, but is a signal to retry an attempt.
 * @author dsmith
 *
 */
public class ConnectionDestroyedException extends GemFireException {
private static final long serialVersionUID = -6918516787578041316L;

  public ConnectionDestroyedException() {
    super();
  }

  public ConnectionDestroyedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionDestroyedException(String message) {
    super(message);
  }

  public ConnectionDestroyedException(Throwable cause) {
    super(cause);
  }

  
}
