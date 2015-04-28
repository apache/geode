/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.GemFireException;

/**
 * The VisualVmNotFoundException class is a GemFireException (RuntimeException) indicating that the JDK jvisualvm
 * tool could not be found on the system.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class VisualVmNotFoundException extends GemFireException {

  public VisualVmNotFoundException() {
  }

  public VisualVmNotFoundException(final String message) {
    super(message);
  }

  public VisualVmNotFoundException(final Throwable cause) {
    super(cause);
  }

  public VisualVmNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
