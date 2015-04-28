/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire;

/**
 * A <code>LicenseException</code> is thrown when
 * the license check fails.
 *
 * @deprecated Licensing is not supported as of 8.0.
 */
@Deprecated
public class LicenseException extends GemFireException {
private static final long serialVersionUID = -1178557127300465801L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>LicenseException</code>.
   */
  public LicenseException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>LicenseException</code> that was
   * caused by a given exception
   */
  public LicenseException(String message, Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>LicenseException</code> that was
   * caused by a given exception
   */
  public LicenseException(Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
