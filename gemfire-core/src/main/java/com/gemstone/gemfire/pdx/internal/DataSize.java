/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

/**
 * This is a helper class that defines the size of the
 * primitive java types.
 * 
 * @author agingade
 * @since 6.6
 */
public class DataSize {

  private DataSize() {
    // no instances allowed
  }
  
  public static final int BYTE_SIZE = 1;
  
  public static final int BOOLEAN_SIZE = 1;
  
  public static final int CHAR_SIZE = 2;

  public static final int SHORT_SIZE = 2;
  
  public static final int INTEGER_SIZE = 4;
  
  public static final int FLOAT_SIZE = 4;
  
  public static final int LONG_SIZE = 8;
  
  public static final int DOUBLE_SIZE = 8;
  
  public static final int DATE_SIZE = 8;
}
