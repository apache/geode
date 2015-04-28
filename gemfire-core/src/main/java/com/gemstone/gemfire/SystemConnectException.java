/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * An <code>SystemConnectException</code> is thrown when a
 * GemFire application tries to connect to an
 * existing distributed system and is unable to contact all members of
 * the distributed system to announce its presence.  This is usually due
 * to resource depletion problems (low memory or too few file descriptors)
 * in other processes.
 */
public class SystemConnectException extends GemFireException {
private static final long serialVersionUID = -7378174428634468238L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemConnectException</code>.
   */
  public SystemConnectException(String message) {
    super(message);
  }
  
  public SystemConnectException(String message, Throwable cause) {
    super(message, cause);
  }
}
