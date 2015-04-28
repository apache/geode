/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.execute;

import com.gemstone.gemfire.distributed.DistributedMember;


/**
 * Thrown if one of the function execution nodes goes away or cache is closed.
 * Function needs to be re-executed if the
 * {@link FunctionException#getCause()} is FunctionInvocationTargetException.
 * 
 * @author Yogesh Mahajan
 * @since 6.0
 * 
 */
public class FunctionInvocationTargetException extends FunctionException {

  private static final long serialVersionUID = 1L;
  
  private DistributedMember id;
  
  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param cause
   *                a Throwable cause of this exception
   */
  public FunctionInvocationTargetException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param id
   *                the DistributedMember id of the source
   * @since 6.5
   * 
   */
  public FunctionInvocationTargetException(String msg, DistributedMember id) {
    super(msg);
    this.id = id;
  }

  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                Exception message
   */
  public FunctionInvocationTargetException(String msg) {
    super(msg);
  }
  /**
   * Construct an instance of FunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param cause
   *                a Throwable cause of this exception
   */
  public FunctionInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Method to get the member id of the Exception
   * 
   * @return DistributedMember id
   * @since 6.5
   */
  public DistributedMember getMemberId() {
    return this.id;
  }

}
