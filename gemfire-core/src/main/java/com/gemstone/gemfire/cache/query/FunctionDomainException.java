/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query;

/**
 * Thrown if the domain of a function is not legal.
 *
 * @author      Eric Zoerner
 * @since 4.0
 */

public class FunctionDomainException extends QueryException {
private static final long serialVersionUID = 1198115662851760423L;
  
  /**
   * Constructs and instance of FunctionDomainException.
   * @param msg error message
   */
  public FunctionDomainException(String msg) {
    super(msg);
  }
}
