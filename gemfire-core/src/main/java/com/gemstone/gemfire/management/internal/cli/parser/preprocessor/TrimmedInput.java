/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

/**
 * Used for trimming input before Pre-processing
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 * 
 */
public class TrimmedInput {
  private final int noOfSpacesRemoved;
  private final String string;

  public TrimmedInput(String string, int noOfSpacesRemoved) {
    this.string = string;
    this.noOfSpacesRemoved = noOfSpacesRemoved;
  }

  public String getString() {
    return string;
  }

  public int getNoOfSpacesRemoved() {
    return noOfSpacesRemoved;
  }

  @Override
  public String toString() {
    return string;
  }
}
