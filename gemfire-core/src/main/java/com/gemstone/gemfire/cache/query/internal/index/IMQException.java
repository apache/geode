/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexMaintainanceQueryException.java
 *
 * Created on March 8, 2005, 6:11 PM
 */

package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.query.QueryException;

/**
 *
 * @author vaibhav
 */
public class IMQException extends QueryException {
private static final long serialVersionUID = -5012914292321850775L;
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public IMQException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param msg the error message
   */
  public IMQException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor used by concrete subclasses
   * @param cause a Throwable cause of this exception
   */
  public IMQException(Throwable cause) {
    super(cause);
  }
  
}
