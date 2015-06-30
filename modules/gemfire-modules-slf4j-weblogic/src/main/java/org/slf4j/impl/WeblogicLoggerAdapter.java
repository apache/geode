/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package org.slf4j.impl;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;
import weblogic.logging.NonCatalogLogger;

/**
 * Very basic class which bridges SLF4J to Weblogic's NonCatalogLogger
 */
public class WeblogicLoggerAdapter extends MarkerIgnoringBase {

  private static final long serialVersionUID = 2372632822791L;

  static String SELF = WeblogicLoggerAdapter.class.getName();

  static String SUPER = MarkerIgnoringBase.class.getName();

  final NonCatalogLogger logger;

  /**
   * WeblogicLoggerAdapter constructor should have only package access so that
   * only WeblogicLoggerAdapter be able to create one.
   */
  WeblogicLoggerAdapter(NonCatalogLogger logger) {
    this.logger = logger;
    this.name = logger.toString();
  }

  /**
   * Is this logger instance enabled for the FINEST level?
   *
   * @return Always true
   */
  @Override
  public boolean isTraceEnabled() {
    return true;

  }

  /**
   * Log a message object at level TRACE.
   *
   * @param msg - the message object to be logged
   */
  @Override
  public void trace(String msg) {
    logger.trace(msg);
  }

  /**
   * Log a message at level TRACE according to the specified format and
   * argument.
   */
  @Override
  public void trace(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    logger.trace(ft.getMessage());
  }

  /**
   * Log a message at level TRACE according to the specified format and
   * arguments.
   */
  @Override
  public void trace(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    logger.trace(ft.getMessage());
  }

  /**
   * Log a message at level TRACE according to the specified format and
   * arguments.
   */
  @Override
  public void trace(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    logger.trace(ft.getMessage());
  }

  /**
   * Log an exception (throwable) at level FINEST with an accompanying message.
   */
  @Override
  public void trace(String msg, Throwable t) {
    logger.trace(msg, t);
  }

  /**
   * Is this logger instance enabled for the DEBUG level?
   *
   * @return Always true
   */
  @Override
  public boolean isDebugEnabled() {
    return true;
  }

  /**
   * Log a message object at level FINE.
   *
   * @param msg - the message object to be logged
   */
  @Override
  public void debug(String msg) {
    logger.debug(msg);
  }

  /**
   * Log a message at level DEBUG according to the specified format and
   * argument.
   */
  @Override
  public void debug(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    logger.debug(ft.getMessage());
  }

  /**
   * Log a message at level DEBUG according to the specified format and
   * arguments.
   */
  @Override
  public void debug(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    logger.debug(ft.getMessage());
  }

  /**
   * Log a message at level FINE according to the specified format and
   * arguments.
   */
  @Override
  public void debug(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    logger.debug(ft.getMessage());
  }

  /**
   * Log an exception (throwable) at level FINE with an accompanying message.
   */
  @Override
  public void debug(String msg, Throwable t) {
    logger.debug(msg, t);
  }

  /**
   * Is this logger instance enabled for the INFO level?
   *
   * @return Always true
   */
  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  /**
   * Log a message object at the INFO level.
   */
  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  /**
   * Log a message at level INFO according to the specified format and
   * argument.
   */
  @Override
  public void info(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    logger.info(ft.getMessage());
  }

  /**
   * Log a message at the INFO level according to the specified format and
   * arguments.
   */
  @Override
  public void info(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    logger.info(ft.getMessage());
  }

  /**
   * Log a message at level INFO according to the specified format and
   * arguments.
   */
  @Override
  public void info(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    logger.info(ft.getMessage());
  }

  /**
   * Log an exception (throwable) at the INFO level with an accompanying
   * message.
   */
  @Override
  public void info(String msg, Throwable t) {
    logger.info(msg, t);
  }

  /**
   * Is this logger instance enabled for the WARNING level?
   *
   * @return Always true
   */
  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  /**
   * Log a message object at the WARNING level.
   */
  @Override
  public void warn(String msg) {
    logger.warning(msg);
  }

  /**
   * Log a message at the WARNING level according to the specified format and
   * argument.
   */
  @Override
  public void warn(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    logger.warning(ft.getMessage());
  }

  /**
   * Log a message at the WARNING level according to the specified format and
   * arguments.
   */
  @Override
  public void warn(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    logger.warning(ft.getMessage());
  }

  /**
   * Log a message at level WARNING according to the specified format and
   * arguments.
   */
  @Override
  public void warn(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    logger.warning(ft.getMessage());
  }

  /**
   * Log an exception (throwable) at the WARNING level with an accompanying
   * message.
   */
  @Override
  public void warn(String msg, Throwable t) {
    logger.warning(msg, t);
  }

  /**
   * Is this logger instance enabled for level SEVERE?
   *
   * @return Always true;
   */
  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  /**
   * Log a message object at the ERROR level.
   */
  @Override
  public void error(String msg) {
    logger.error(msg);
  }

  /**
   * Log a message at the ERROR level according to the specified format and
   * argument.
   */
  @Override
  public void error(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    logger.error(ft.getMessage());
  }

  /**
   * Log a message at the ERROR level according to the specified format and
   * arguments.
   */
  @Override
  public void error(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    logger.error(ft.getMessage());
  }

  /**
   * Log a message at level ERROR according to the specified format and
   * arguments.
   */
  @Override
  public void error(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    logger.error(ft.getMessage());
  }

  /**
   * Log an exception (throwable) at the ERROR level with an accompanying
   * message.
   */
  @Override
  public void error(String msg, Throwable t) {
    logger.error(msg, t);
  }
}
