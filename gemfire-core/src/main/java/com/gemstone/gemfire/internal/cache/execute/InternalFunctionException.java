/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;

/**
 * This is an exception used internally when the function sends the exception.
 * Exception sent through ResultSender.sendException will be wrapped
 * internally in InternalFunctionException. This InternalFunctionException will
 * be used to decide whether the exception should be added as a part of
 * addResult or exception should be thrown while doing ResultCollector#getResult
 * 
 * <p>
 * The exception string provides details on the cause of failure.
 * </p>
 * 
 * @author Kishor Bachhav
 * 
 * @since 6.6
 * @see FunctionService
 */

public class InternalFunctionException extends FunctionException {

  /**
   * Creates new internal function exception with given error message.
   * 
   */
  public InternalFunctionException() {
  }

  /**
   * Creates new internal function exception with given error message.
   * 
   * @param msg
   */
  public InternalFunctionException(String msg) {
    super(msg);
  }

  /**
   * Creates new internal function exception with given error message and optional nested
   * exception.
   * 
   * @param msg
   * @param cause
   */
  public InternalFunctionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Creates new internal function exception given throwable as a cause and source of
   * error message.
   * 
   * @param cause
   */
  public InternalFunctionException(Throwable cause) {
    super(cause);
  }
}
