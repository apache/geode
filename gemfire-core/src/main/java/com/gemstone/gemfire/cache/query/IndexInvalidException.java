/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexInvalidException.java
 *
 * Created on March 16, 2005, 7:40 PM
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the index definition is not valid.
 *
 * @author vaibhav
 * @since 4.0
 */

public class IndexInvalidException extends GemFireException {
private static final long serialVersionUID = 3285601274732772770L;
  
  /**
   * Construct an instance of IndexInvalidException
   * @param msg the error message
   */
  public IndexInvalidException(String msg) {
    super(msg);
  }
  
  /**
   * Construct an instance of IndexInvalidException
   * @param msg the error message
   * @param cause a Throwable cause
   */
  public IndexInvalidException(String msg, Throwable cause) {
    super(msg);
    initCause(cause);
  }
  
  /**
   * Construct an instance of IndexInvalidException
   * @param cause a Throwable cause
   */
  public IndexInvalidException(Throwable cause) {
    super(cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
    initCause(cause);
  }
  
}
