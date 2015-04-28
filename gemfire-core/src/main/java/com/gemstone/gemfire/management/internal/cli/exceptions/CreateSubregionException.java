/*
 * ========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.exceptions;

/**
 * Indicates inability to create a Subregion of a region.
 *
 * @author Abhishek Chaudhari
 * @since 8.0
 */
// TODO - Abhishek - Include in GemFire Exception Enhancements
public class CreateSubregionException extends RuntimeException {

  private static final long serialVersionUID = 4387344870743824916L;

  public CreateSubregionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CreateSubregionException(String message) {
    super(message);
  }

  public CreateSubregionException(Throwable cause) {
    super(cause);
  }
}
