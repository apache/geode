/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

/**
 * A ConnectionFailedException is thrown if connection to a process fails.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class ConnectionFailedException extends Exception {
  private static final long serialVersionUID = 5622636452836752700L;

  /**
   * Creates a new <code>ConnectionFailedException</code>.
   */
  public ConnectionFailedException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>ConnectionFailedException</code> that was
   * caused by a given exception
   */
  public ConnectionFailedException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>ConnectionFailedException</code> that was
   * caused by a given exception
   */
  public ConnectionFailedException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
