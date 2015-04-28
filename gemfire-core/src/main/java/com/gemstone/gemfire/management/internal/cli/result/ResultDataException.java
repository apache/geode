/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class ResultDataException extends RuntimeException {

  private static final long serialVersionUID = 3851919811942980944L;

  /**
   * @param message
   */
  public ResultDataException(String message) {
    super(message);
  }

}
