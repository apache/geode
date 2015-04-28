/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

/**
 * A FileAlreadyExistsException is thrown when a pid file already exists
 * and the launcher expects to create a new pid file without forcing the
 * deletion of the old one.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class FileAlreadyExistsException extends Exception {
  private static final long serialVersionUID = 5471082555536094256L;

  /**
   * Creates a new <code>FileAlreadyExistsException</code>.
   */
  public FileAlreadyExistsException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>FileAlreadyExistsException</code> that was
   * caused by a given exception
   */
  public FileAlreadyExistsException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>FileAlreadyExistsException</code> that was
   * caused by a given exception
   */
  public FileAlreadyExistsException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
