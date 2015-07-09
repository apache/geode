/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.persistence.logging;

/**
 * Abstract class that formats LogRecords
 */
public abstract class Formatter {

  /** Should we print a stack trace along with logging messages */
  protected static boolean STACK_TRACE =
    Boolean.getBoolean("com.gemstone.persistence.logging.StackTraces");

  /**
   * Formats the given log record as a String
   */
  public abstract String format(LogRecord record);

  /**
   * Formats the message string from a log record
   */
  public String formatMessage(LogRecord record) {
    // Simple
    return(record.getMessage());
  }

}
