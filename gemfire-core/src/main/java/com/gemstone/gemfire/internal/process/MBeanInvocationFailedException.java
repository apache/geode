/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

/**
 * A MBeanInvocationFailedException is thrown if invocation of the mbean failed.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class MBeanInvocationFailedException extends Exception {
  private static final long serialVersionUID = 7991096466859690801L;

  /**
   * Creates a new <code>MBeanInvocationFailedException</code>.
   */
  public MBeanInvocationFailedException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>MBeanInvocationFailedException</code> that was
   * caused by a given exception
   */
  public MBeanInvocationFailedException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>MBeanInvocationFailedException</code> that was
   * caused by a given exception
   */
  public MBeanInvocationFailedException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
