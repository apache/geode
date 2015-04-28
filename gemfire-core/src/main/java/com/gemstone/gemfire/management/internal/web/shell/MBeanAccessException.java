/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.shell;

/**
 * The MBeanAccessException class is a RuntimeException indicating that an attempt to access an MBean attribute or
 * invocation of an MBean operation failed.
 * <p/>
 * @author John Blum
 * @see java.lang.RuntimeException
 * @since 8.0
 */
@SuppressWarnings("unused")
public class MBeanAccessException extends RuntimeException {

  public MBeanAccessException() {
  }

  public MBeanAccessException(final String message) {
    super(message);
  }

  public MBeanAccessException(final Throwable cause) {
    super(cause);
  }

  public MBeanAccessException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
