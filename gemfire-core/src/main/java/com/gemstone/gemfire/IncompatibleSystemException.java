/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>IncompatibleSystemException</code> is thrown when a
 * new GemFire process tries to connect to an
 * existing distributed system and its version is not the same as
 * that of the distributed system. In this case the new member is
 * not allowed to connect to the distributed system.
 * <p>As of GemFire 5.0 this exception should be named IncompatibleDistributedSystemException
 */
public class IncompatibleSystemException extends GemFireException {
private static final long serialVersionUID = -6852188720149734350L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>IncompatibleSystemException</code>.
   */
  public IncompatibleSystemException(String message) {
    super(message);
  }
}
