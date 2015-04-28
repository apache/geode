/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.OperationAbortedException;

/**
 * A <code>ClientNotReadyException</code> indicates a client attempted to invoke
 * the {@link com.gemstone.gemfire.cache.Cache#readyForEvents}
 * method, but failed.
 * <p>This exception was moved from the <code>util</code> package in 5.7.
 * 
 * @author darrel
 *
 * @since 5.7
 * @deprecated as of 6.5 this exception is no longer thrown by GemFire so any code that catches it should be removed.
 * 
 */
public class ClientNotReadyException extends OperationAbortedException {
private static final long serialVersionUID = -315765802919271588L;
  /**
   * Constructs an instance of <code>ClientNotReadyException</code> with the
   * specified detail message.
   * 
   * @param msg the detail message
   */
  public ClientNotReadyException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>ClientNotReadyException</code> with the
   * specified detail message and cause.
   * 
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public ClientNotReadyException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
