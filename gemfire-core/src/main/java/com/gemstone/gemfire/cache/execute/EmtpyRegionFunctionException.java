/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.execute;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Exception to indicate that Region is empty for data aware functions.
 * 
 * @author skumar
 * @since 6.5
 * 
 */
public class EmtpyRegionFunctionException extends FunctionException {

  private static final long serialVersionUID = 1L;

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param cause
   *                a Throwable cause of this exception
   */
  public EmtpyRegionFunctionException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param msg
   *                Exception message
   */
  public EmtpyRegionFunctionException(String msg) {
    super(msg);
  }

  /**
   * Construct an instance of EmtpyRegionFunctionException
   * 
   * @param msg
   *                the error message
   * @param cause
   *                a Throwable cause of this exception
   */
  public EmtpyRegionFunctionException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
