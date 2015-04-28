/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the query language syntax is not valid.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class QueryInvalidException extends GemFireException {
private static final long serialVersionUID = 2849255122285215114L;
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   */
  public QueryInvalidException(String msg) {
    super(msg);
  }
  
  /**
   * Construct an instance of QueryInvalidException
   * @param msg the error message
   * @param cause a Throwable cause
   */
  public QueryInvalidException(String msg, Throwable cause) {
    super(msg);
    initCause(cause);
  }
}
