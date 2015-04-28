/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.shared;

/**
 * Encapsulates an error in invoking OS native calls. A counterpart of JNA's
 * <code>LastErrorException</code> so as to not expose the JNA
 * <code>LastErrorException</code> class, and also for ODBC/.NET drivers that
 * don't use JNA.
 * 
 * @author swale
 * @since 8.0
 */
public class NativeErrorException extends Exception {

  private static final long serialVersionUID = -1417824976407332942L;

  private final int errorCode;

  public NativeErrorException(String msg, int errorCode, Throwable cause) {
    super(msg, cause);
    this.errorCode = errorCode;
  }

  public final int getErrorCode() {
    return this.errorCode;
  }
}
