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
 * The JConsoleNotFoundException class is a RuntimeException class that indicates that the JDK JConsole tool could
 * not be located in the file system.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class JConsoleNotFoundException extends GemFireException {

  public JConsoleNotFoundException() {
  }

  public JConsoleNotFoundException(final String message) {
    super(message);
  }
  public JConsoleNotFoundException(final Throwable cause) {
    super(cause);
  }

  public JConsoleNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
