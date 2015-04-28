/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * Thrown when the number of bound paramters for a query does not match the
 * number of placeholders.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public class ParameterCountInvalidException extends QueryException {
private static final long serialVersionUID = -3249156440150789428L;
  
  /**
   * Creates a new instance of QueryParameterCountInvalidException
   * @param message the error message
   */
  public ParameterCountInvalidException(String message) {
    super(message);
  }
  
}
