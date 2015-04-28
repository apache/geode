/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.util;

/**
 * The SunAPINotFoundException class is a RuntimeException indicating that the Sun API classes and components could
 * not be found, which is most likely the case when we are not running a Sun JVM (like HotSpot).
 * </p>
 * @author John Blum
 * @see java.lang.RuntimeException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class SunAPINotFoundException extends RuntimeException {

  public SunAPINotFoundException() {
  }

  public SunAPINotFoundException(final String message) {
    super(message);
  }

  public SunAPINotFoundException(final Throwable t) {
    super(t);
  }

  public SunAPINotFoundException(final String message, final Throwable t) {
    super(message, t);
  }

}
