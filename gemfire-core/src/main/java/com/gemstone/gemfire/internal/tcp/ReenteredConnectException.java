/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.GemFireException;

/**
 * An exception indicating that the same thread that is in the middle
 * of trying to connect has tried to obtain a connection to the same
 * member further down the call stack.
 * 
 * This condition has been observered when using an AlertListener, because 
 * we try to transmit messages logged during a connection to the very member
 * we're trying to connect to. 
 * @author dsmith
 *
 */
public class ReenteredConnectException extends GemFireException {

  public ReenteredConnectException() {
    super();
  }

  public ReenteredConnectException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReenteredConnectException(String message) {
    super(message);
  }

  public ReenteredConnectException(Throwable cause) {
    super(cause);
  }


}
