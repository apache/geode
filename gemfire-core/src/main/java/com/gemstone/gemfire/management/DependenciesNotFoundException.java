/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.GemFireException;

/**
 * Indicates that required dependencies were not found in the ClassPath.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class DependenciesNotFoundException extends GemFireException {
  private static final long serialVersionUID = 9082304929238159814L;

  /**
   * Constructs a new DependenciesNotFoundException with the specified detail 
   * message and cause.
   *
   * Note that the detail message associated with <code>cause</code> is
   * <i>not</i> automatically incorporated in this runtime exception's detail
   * message.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public DependenciesNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new DependenciesNotFoundException with the specified detail 
   * message.
   * 
   * @param message
   *          The detail message.
   */
  public DependenciesNotFoundException(String message) {
    super(message);
  }
}
