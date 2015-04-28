/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire;

/**
 * A <code>UnmodifiableException</code> is thrown when a
 * an attempt is made to modify a GemFire member configuration attribute
 * that can not currently be modified. In most cases the reason it can
 * not be modified is that the member is active.
 */
public class UnmodifiableException extends GemFireException {
private static final long serialVersionUID = -1043243260052395455L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>UnmodifiableException</code>.
   */
  public UnmodifiableException(String message) {
    super(message);
  }
}
