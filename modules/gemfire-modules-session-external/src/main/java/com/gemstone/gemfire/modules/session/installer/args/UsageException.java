/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.installer.args;

/**
 * Invalid usage exception.
 */
public class UsageException extends Exception {

  /**
   * Serial format version.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Stored usage message.
   */
  private String usage;

  /**
   * Creates a new UsageException.
   */
  public UsageException() {
    super();
  }

  /**
   * Creates a new UsageException.
   *
   * @param message description of exceptional condition
   */
  public UsageException(final String message) {
    super(message);
  }

  /**
   * Creates a new UsageException.
   *
   * @param message description of exceptional condition
   * @param cause   provoking exception
   */
  public UsageException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new UsageException.
   *
   * @param cause provoking exception
   */
  public UsageException(final Throwable cause) {
    super(cause);
  }


  /**
   * Attaches a usage message to the exception for later consumption.
   *
   * @param usageText text to display to user to guide them to correct usage.
   *                  This is generated and set by the <code>ArgsProcessor</code>.
   */
  public void setUsage(final String usageText) {
    usage = usageText;
  }

  /**
   * Returns the usage message previously set.
   *
   * @return message or null if not set.
   */
  public String getUsage() {
    return usage;
  }
}
