/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.cache.OperationAbortedException;

/**
 * Exception to inform user that AsyncEventQueue is wrongly configured.
 *  
 * @author Suranjan Kumar
 *
 */
public class AsyncEventQueueConfigurationException extends
    OperationAbortedException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructor.
   * Creates a new instance of <code>AsyncEventQueueConfigurationException</code>.
   */
  public AsyncEventQueueConfigurationException() {
    super();
  }

  /**
   * Constructor.
   * Creates an instance of <code>AsyncEventQueueConfigurationException</code> with the
   * specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  
  public AsyncEventQueueConfigurationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructor.
   * Creates an instance of <code>GatewaySenderException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public AsyncEventQueueConfigurationException(String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * Creates an instance of <code>AsyncEventQueueConfigurationException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   */
  public AsyncEventQueueConfigurationException(Throwable cause) {
    super(cause);
  }

}
