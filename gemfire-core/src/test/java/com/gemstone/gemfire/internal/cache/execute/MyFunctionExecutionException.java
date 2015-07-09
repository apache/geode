/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;


public class MyFunctionExecutionException extends RuntimeException{
  
  /**
   * Creates new exception with given error message.
   * 
   */
  public MyFunctionExecutionException() {
  }

  /**
   * Creates new exception with given error message.
   * 
   * @param msg
   */
  public MyFunctionExecutionException(String msg) {
    super(msg);
  }

  /**
   * Creates new exception with given error message and optional nested
   * exception.
   * 
   * @param msg
   * @param cause
   */
  public MyFunctionExecutionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Creates new exception given Throwable as a cause and source of
   * error message.
   * 
   * @param cause
   */
  public MyFunctionExecutionException(Throwable cause) {
    super(cause);
  }

}
