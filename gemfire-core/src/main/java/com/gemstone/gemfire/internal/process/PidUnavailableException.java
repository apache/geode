/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

/**
 * A PidUnavailableException is thrown when the pid cannot be parsed from
 * the RuntimeMXBean name or otherwise determined.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class PidUnavailableException extends Exception {
  private static final long serialVersionUID = -1660269538268828059L;

  /**
   * Creates a new <code>PidUnavailableException</code>.
   */
  public PidUnavailableException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>PidUnavailableException</code> that was
   * caused by a given exception
   */
  public PidUnavailableException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>PidUnavailableException</code> that was
   * caused by a given exception
   */
  public PidUnavailableException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
