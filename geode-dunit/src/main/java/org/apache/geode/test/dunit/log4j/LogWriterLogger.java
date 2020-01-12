/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.dunit.log4j;

import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.ALL;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.ERROR;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.FINE;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.FINER;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.FINEST;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.INFO;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.NONE;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.SEVERE;
import static org.apache.geode.test.dunit.log4j.LogWriterLogger.LogWriterLevel.WARNING;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.BreakIterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import org.apache.geode.GemFireException;
import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.logging.SecurityLogWriter;

/**
 * Implements GemFireLogger with custom levels while also bridging LogWriter and LogWriterI18n to
 * Log4J.
 */
@SuppressWarnings("unused")
public class LogWriterLogger extends ExtendedLoggerWrapper implements LogWriter, LogWriterI18n {

  public static final String SECURITY_PREFIX = DistributionConfig.SECURITY_PREFIX_NAME;

  private final ExtendedLoggerWrapper logWrapper;
  private final String connectionName;
  private final String loggerName;
  private final boolean isSecure;

  private LogWriterLogger(final Logger logger, final String connectionName,
      final boolean isSecure) {
    super((AbstractLogger) logger, logger.getName(), logger.getMessageFactory());
    logWrapper = this;
    this.connectionName = connectionName;
    loggerName = getName();
    this.isSecure = isSecure;
  }

  /**
   * Returns a custom Logger with the specified name and null connectionName.
   *
   * @param name The this.logger name. If null the name of the calling class will be used.
   * @param isSecure True if creating a Logger for security logging.
   * @return The custom Logger.
   */
  public static LogWriterLogger create(final String name, final boolean isSecure) {
    return create(name, null, isSecure);
  }

  /**
   * Returns a custom Logger with the specified name.
   *
   * @param name The this.logger name. If null the name of the calling class will be used.
   * @param connectionName The member name (also known as connection name)
   * @param isSecure True if creating a Logger for security logging.
   * @return The custom Logger.
   */
  public static LogWriterLogger create(final String name, final String connectionName,
      final boolean isSecure) {
    Logger wrapped = LogManager.getLogger(name);
    return new LogWriterLogger(wrapped, connectionName, isSecure);
  }

  public static LogWriterLogger create(final Logger logger) {
    return new LogWriterLogger(logger, null, false);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void finest(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void finest(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void finest(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void finest(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void finest(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   */
  public void finest(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void finest(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  public void finest(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finest(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void finest(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void finest(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  @Override
  public void finest(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void finer(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void finer(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void finer(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void finer(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void finer(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   */
  public void finer(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void finer(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  public void finer(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void finer(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void finer(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  private void finer(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  @Override
  public void finer(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void fine(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void fine(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void fine(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void fine(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void fine(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   *
   * @param message the message string to be logged
   */
  public void fine(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void fine(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param message the message object to log.
   */
  public void fine(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void fine(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void fine(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.DEBUG} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void fine(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  @Override
  public void fine(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, throwable);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void config(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void config(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void config(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void config(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void config(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param message the message string to be logged
   */
  public void config(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void config(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param message the message object to log.
   */
  public void config(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void config(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void config(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void config(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  @Override
  public void config(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, throwable);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param msg the message string to be logged
   */
  @Override
  public void info(final Marker marker, final Message msg) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, msg, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param msg the message string to be logged
   * @param t A Throwable or null.
   */
  @Override
  public void info(final Marker marker, final Message msg, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
  public void info(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void info(final Marker marker, final Object message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
  public void info(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param params parameters to the message.
   */
  @Override
  public void info(final Marker marker, final String message, final Object... params) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void info(final Marker marker, final String message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param msg the message string to be logged
   */
  @Override
  public void info(final Message msg) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, msg, null);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param msg the message string to be logged
   * @param t A Throwable or null.
   */
  @Override
  public void info(final Message msg, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void info(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void info(final Object message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void info(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param params parameters to the message.
   */
  @Override
  public void info(final String message, final Object... params) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void info(final String message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void warning(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void warning(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void warning(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void warning(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void warning(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   *
   * @param message the message string to be logged
   */
  public void warning(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void warning(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param message the message object to log.
   */
  public void warning(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void warning(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void warning(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.WARN} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void warning(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  @Override
  public void warning(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, throwable);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param msg the message string to be logged
   */
  @Override
  public void error(final Marker marker, final Message msg) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, msg, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.ERROR} level.
   *
   * @param marker the marker data specific to this log statement
   * @param msg the message string to be logged
   * @param t A Throwable or null.
   */
  @Override
  public void error(final Marker marker, final Message msg, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
  public void error(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void error(final Marker marker, final Object message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
  public void error(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.ERROR} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param params parameters to the message.
   */
  @Override
  public void error(final Marker marker, final String message, final Object... params) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void error(final Marker marker, final String message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.ERROR} level.
   *
   * @param msg the message string to be logged
   */
  @Override
  public void error(final Message msg) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, msg, null);
  }

  /**
   * Logs the specified Message at the {@code Level.ERROR} level.
   *
   * @param msg the message string to be logged
   * @param t A Throwable or null.
   */
  @Override
  public void error(final Message msg, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void error(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void error(final Object message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void error(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.ERROR} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param params parameters to the message.
   */
  @Override
  public void error(final String message, final Object... params) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param t the exception to log, including its stack trace.
   */
  @Override
  public void error(final String message, final Throwable t) {
    logWrapper.logIfEnabled(loggerName, Level.ERROR, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  public void severe(final Marker marker, final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void severe(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void severe(final Marker marker, final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, null);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  public void severe(final Marker marker, final String message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void severe(final Marker marker, final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   *
   * @param message the message string to be logged
   */
  public void severe(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  public void severe(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param message the message object to log.
   */
  public void severe(final Object message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, null);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  public void severe(final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param message the message object to log.
   */
  @Override
  public void severe(final String message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.FATAL} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  public void severe(final String message, final Object... parameters) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, parameters);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack
   */
  @Override
  public void severe(final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, throwable);
  }

  public void log(int logWriterLevel, final String message, final Throwable throwable) {
    Level level = LogWriterLevelConverter.toLevel(LogWriterLevel.find(logWriterLevel));
    logWrapper.logIfEnabled(loggerName, level, null, message, throwable);
  }

  @Override
  public boolean severeEnabled() {
    return isEnabled(Level.FATAL);
  }

  @Override
  public boolean errorEnabled() {
    return isEnabled(Level.ERROR);
  }

  @Override
  public boolean warningEnabled() {
    return isEnabled(Level.WARN);
  }

  @Override
  public boolean infoEnabled() {
    return isEnabled(Level.INFO);
  }

  @Override
  public boolean configEnabled() {
    return isEnabled(Level.INFO);
  }

  @Override
  public boolean fineEnabled() {
    return isEnabled(Level.DEBUG);
  }

  @Override
  public boolean finerEnabled() {
    return isEnabled(Level.TRACE);
  }

  @Override
  public boolean finestEnabled() {
    return isEnabled(Level.TRACE);
  }

  @Override
  public void entering(String sourceClass, String sourceMethod) {
    finer("ENTRY {}:{}", sourceClass, sourceMethod);
  }

  @Override
  public void exiting(String sourceClass, String sourceMethod) {
    finer("RETURN {}:{}", sourceClass, sourceMethod);
  }

  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    finer("THROW {}:{}", sourceClass, sourceMethod, thrown);
  }

  @Override
  public Handler getHandler() {
    return new GemFireHandler(this);
  }

  @Override
  public LogWriterI18n convertToLogWriterI18n() {
    return this;
  }

  @Override
  public void severe(Throwable throwable) {
    severe((String) null, throwable);
  }

  @Override
  public void error(Throwable throwable) {
    error((String) null, throwable);
  }

  @Override
  public void warning(Throwable throwable) {
    warning((String) null, throwable);
  }

  @Override
  public void info(Throwable throwable) {
    info((String) null, throwable);
  }

  @Override
  public void config(Throwable throwable) {
    config((String) null, throwable);
  }

  @Override
  public void fine(Throwable throwable) {
    fine((String) null, throwable);
  }

  @Override
  public void finer(Throwable throwable) {
    finer((String) null, throwable);
  }

  @Override
  public void finest(Throwable throwable) {
    finest((String) null, throwable);
  }

  @Override
  public void severe(StringId messageId, Object[] parameters, Throwable throwable) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString(parameters), throwable);
    }
  }

  @Override
  public void severe(StringId messageId, Object parameter, Throwable throwable) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString(parameter), throwable);
    }
  }

  @Override
  public void severe(StringId messageId, Throwable throwable) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString(), throwable);
    }
  }

  @Override
  public void severe(StringId messageId, Object[] parameters) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString(parameters));
    }
  }

  @Override
  public void severe(StringId messageId, Object parameter) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString(parameter));
    }
  }

  @Override
  public void severe(StringId messageId) {
    if (isEnabled(Level.FATAL)) {
      severe(messageId.toLocalizedString());
    }
  }

  @Override
  public void error(StringId messageId, Object[] parameters, Throwable throwable) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString(parameters), throwable);
    }
  }

  @Override
  public void error(StringId messageId, Object parameter, Throwable throwable) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString(parameter), throwable);
    }
  }

  @Override
  public void error(StringId messageId, Throwable throwable) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString(), throwable);
    }
  }

  @Override
  public void error(StringId messageId, Object[] parameters) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString(parameters));
    }
  }

  @Override
  public void error(StringId messageId, Object parameter) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString(parameter));
    }
  }

  @Override
  public void error(StringId messageId) {
    if (isEnabled(Level.ERROR)) {
      error(messageId.toLocalizedString());
    }
  }

  @Override
  public void warning(StringId messageId, Object[] parameters, Throwable throwable) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString(parameters), throwable);
    }
  }

  @Override
  public void warning(StringId messageId, Object parameter, Throwable throwable) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString(parameter), throwable);
    }
  }

  @Override
  public void warning(StringId messageId, Throwable throwable) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString(), throwable);
    }
  }

  @Override
  public void warning(StringId messageId, Object[] parameters) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString(parameters));
    }
  }

  @Override
  public void warning(StringId messageId, Object parameter) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString(parameter));
    }
  }

  @Override
  public void warning(StringId messageId) {
    if (isEnabled(Level.WARN)) {
      warning(messageId.toLocalizedString());
    }
  }

  @Override
  public void info(StringId messageId, Object[] parameters, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString(parameters), throwable);
    }
  }

  @Override
  public void info(StringId messageId, Object parameter, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString(parameter), throwable);
    }
  }

  @Override
  public void info(StringId messageId, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString(), throwable);
    }
  }

  @Override
  public void info(StringId messageId, Object[] parameters) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString(parameters));
    }
  }

  @Override
  public void info(StringId messageId, Object parameter) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString(parameter));
    }
  }

  @Override
  public void info(StringId messageId) {
    if (isEnabled(Level.INFO)) {
      info(messageId.toLocalizedString());
    }
  }

  @Override
  public void config(StringId messageId, Object[] parameters, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString(parameters), throwable);
    }
  }

  @Override
  public void config(StringId messageId, Object parameter, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString(parameter), throwable);
    }
  }

  @Override
  public void config(StringId messageId, Throwable throwable) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString(), throwable);
    }
  }

  @Override
  public void config(StringId messageId, Object[] parameters) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString(parameters));
    }
  }

  @Override
  public void config(StringId messageId, Object parameter) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString(parameter));
    }
  }

  @Override
  public void config(StringId messageId) {
    if (isEnabled(Level.INFO)) {
      config(messageId.toLocalizedString());
    }
  }

  @Override
  public LogWriter convertToLogWriter() {
    return this;
  }

  public int getLogWriterLevel() {
    final Level log4jLevel = logWrapper.getLevel();

    if (log4jLevel == Level.OFF) {
      return NONE.intLevel();
    } else if (log4jLevel == Level.FATAL) {
      return SEVERE.intLevel();
    } else if (log4jLevel == Level.ERROR) {
      return ERROR.intLevel();
    } else if (log4jLevel == Level.WARN) {
      return WARNING.intLevel();
    } else if (log4jLevel == Level.INFO) {
      return INFO.intLevel();
    } else if (log4jLevel == Level.DEBUG) {
      return FINE.intLevel();
    } else if (log4jLevel == Level.TRACE) {
      return FINER.intLevel();
    } else if (log4jLevel == Level.ALL) {
      return ALL.intLevel();
    }

    throw new IllegalStateException(
        "Level " + log4jLevel + " could not be mapped to LogWriter level.");
  }

  public boolean isSecure() {
    return isSecure;
  }

  public void put(int messageLevel, String message, Throwable throwable) {
    log(messageLevel, message, throwable);
  }

  public void put(int messageLevel, StringId messageId, Object[] parameters, Throwable throwable) {
    log(messageLevel, messageId.toLocalizedString(parameters), throwable);
  }

  public String getConnectionName() {
    return connectionName;
  }

  /**
   * If the writer's level is {@code ALL_LEVEL} then all messages will be logged.
   */
  private static final int ALL_LEVEL = Integer.MIN_VALUE;
  /**
   * If the writer's level is {@code FINEST_LEVEL} then finest, finer, fine, config, info,
   * warning, error, and severe messages will be logged.
   */
  private static final int FINEST_LEVEL = 300;
  /**
   * If the writer's level is {@code FINER_LEVEL} then finer, fine, config, info, warning,
   * error, and severe messages will be logged.
   */
  private static final int FINER_LEVEL = 400;
  /**
   * If the writer's level is {@code FINE_LEVEL} then fine, config, info, warning, error, and
   * severe messages will be logged.
   */
  private static final int FINE_LEVEL = 500;
  /**
   * If the writer's level is {@code CONFIG_LEVEL} then config, info, warning, error, and
   * severe messages will be logged.
   */
  private static final int CONFIG_LEVEL = 700;
  /**
   * If the writer's level is {@code INFO_LEVEL} then info, warning, error, and severe messages
   * will be logged.
   */
  private static final int INFO_LEVEL = 800;
  /**
   * If the writer's level is {@code WARNING_LEVEL} then warning, error, and severe messages
   * will be logged.
   */
  private static final int WARNING_LEVEL = 900;
  /**
   * If the writer's level is {@code SEVERE_LEVEL} then only severe messages will be logged.
   */
  private static final int SEVERE_LEVEL = 1000;
  /**
   * If the writer's level is {@code ERROR_LEVEL} then error and severe messages will be
   * logged.
   */
  private static final int ERROR_LEVEL = (WARNING_LEVEL + SEVERE_LEVEL) / 2;
  /**
   * If the writer's level is {@code NONE_LEVEL} then no messages will be logged.
   */
  private static final int NONE_LEVEL = Integer.MAX_VALUE;

  /**
   * A bit mask to remove any potential flags added to the msgLevel. Intended to be used in
   * {@link #getRealLogLevel}.
   */
  private static final int LOGGING_FLAGS_MASK = 0x00FFFFFF;

  /**
   * A flag to indicate the {@link SecurityLogWriter#SECURITY_PREFIX} should be appended to the log
   * level.
   */
  private static final int SECURITY_LOGGING_FLAG = 0x40000000;

  /**
   * Gets the string representation for the given {@code level} int code.
   */
  public static String levelToString(int level) {
    switch (level) {
      case ALL_LEVEL:
        return "all";
      case FINEST_LEVEL:
        return "finest";
      case FINER_LEVEL:
        return "finer";
      case FINE_LEVEL:
        return "fine";
      case CONFIG_LEVEL:
        return "config";
      case INFO_LEVEL:
        return "info";
      case WARNING_LEVEL:
        return "warning";
      case ERROR_LEVEL:
        return "error";
      case SEVERE_LEVEL:
        return "severe";
      case NONE_LEVEL:
        return "none";
      default:
        return levelToStringSpecialCase(level);
    }
  }

  /**
   * Handles the special cases for {@code #levelToString(int)} including such cases as the
   * SECURITY_LOGGING_PREFIX and an invalid log level.
   */
  private static String levelToStringSpecialCase(int levelWithFlags) {
    if ((levelWithFlags & SECURITY_LOGGING_FLAG) != 0) {
      // We know the flag is set so XOR will zero it out.
      int level = levelWithFlags ^ SECURITY_LOGGING_FLAG;
      return SecurityLogWriter.SECURITY_PREFIX + levelToString(level);
    }
    // Needed to prevent infinite recursion
    // This signifies an unknown log level was used
    return "level-" + levelWithFlags;
  }

  /**
   * Levels used for identifying the severity of a {@link LogWriter} event. From least specific to
   * most:
   * <ul>
   * <li>{@link #ALL} (least specific, all logging)</li>
   * <li>{@link #FINEST} (least specific, a lot of logging)</li>
   * <li>{@link #FINER}</li>
   * <li>{@link #FINE}</li>
   * <li>{@link #CONFIG}</li>
   * <li>{@link #INFO}</li>
   * <li>{@link #WARNING}</li>
   * <li>{@link #ERROR}</li>
   * <li>{@link #SEVERE} (most specific, a little logging)</li>
   * <li>{@link #NONE} (most specific, no logging)</li>
   * </ul>
   *
   * <p>
   * Default level in Geode is {@link #CONFIG}.
   */
  public enum LogWriterLevel {

    ALL(ALL_LEVEL),
    FINEST(FINEST_LEVEL),
    FINER(FINER_LEVEL),
    FINE(FINE_LEVEL),
    CONFIG(CONFIG_LEVEL), // default
    INFO(INFO_LEVEL),
    WARNING(WARNING_LEVEL),
    ERROR(ERROR_LEVEL),
    SEVERE(SEVERE_LEVEL),
    NONE(NONE_LEVEL);

    public static LogWriterLevel find(final int intLevel) {
      for (LogWriterLevel logWriterLevel : values()) {
        if (logWriterLevel.intLevel == intLevel) {
          return logWriterLevel;
        }
      }
      throw new IllegalArgumentException("No LogWriterLevel found for intLevel " + intLevel);
    }

    private final int intLevel;

    LogWriterLevel(final int intLevel) {
      this.intLevel = intLevel;
    }

    public int intLevel() {
      return intLevel;
    }
  }

  /**
   * Converts between {@link LogWriterLevel}s and Log4J2 {@code Level}s.
   *
   * <p>
   * Implementation note: switch and if-else structures perform better than using a HashMap for this
   * which matters the most if used for every log statement.
   */
  public static class LogWriterLevelConverter {

    /**
     * Converts from a {@link LogWriterLevel} to a Log4J2 {@code Level}.
     *
     * @throws IllegalArgumentException if there is no matching Log4J2 Level
     */
    public static Level toLevel(final LogWriterLevel logWriterLevel) {
      switch (logWriterLevel) {
        case ALL:
          return Level.ALL;
        case SEVERE:
          return Level.FATAL;
        case ERROR:
          return Level.ERROR;
        case WARNING:
          return Level.WARN;
        case INFO:
          return Level.INFO;
        case CONFIG:
          return Level.INFO;
        case FINE:
          return Level.DEBUG;
        case FINER:
          return Level.TRACE;
        case FINEST:
          return Level.TRACE;
        case NONE:
          return Level.OFF;
      }

      throw new IllegalArgumentException("No matching Log4J2 Level for " + logWriterLevel + ".");
    }

    /**
     * Converts from a Log4J2 {@code Level} to a {@link LogWriterLevel}.
     *
     * @throws IllegalArgumentException if there is no matching Alert
     */
    public LogWriterLevel fromLevel(final Level level) {
      if (level == Level.ALL) {
        return ALL;
      } else if (level == Level.FATAL) {
        return SEVERE;
      } else if (level == Level.ERROR) {
        return ERROR;
      } else if (level == Level.WARN) {
        return WARNING;
      } else if (level == Level.INFO) {
        return INFO;
      } else if (level == Level.DEBUG) {
        return FINE;
      } else if (level == Level.TRACE) {
        return FINEST;
      } else if (level == Level.OFF) {
        return NONE;
      }

      throw new IllegalArgumentException("No matching AlertLevel for Log4J2 Level " + level + ".");
    }
  }

  /**
   * Implementation of the standard JDK handler that publishes a log record to a LogWriterImpl. Note
   * this handler ignores any installed handler.
   */
  public static class GemFireHandler extends Handler {

    /**
     * Use the log writer to use some of its formatting code.
     */
    private LogWriter logWriter;

    private GemFireHandler(LogWriter logWriter) {
      this.logWriter = logWriter;
      setFormatter(new GemFireFormatter());
    }

    @Override
    public void close() {
      // clear the reference to GFE LogWriter
      logWriter = null;
    }

    @Override
    public void flush() {
      // nothing needed
    }

    private String getMessage(final LogRecord record) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append('(').append("tid=").append(record.getThreadID())
          .append(" msgId=").append(record.getSequenceNumber()).append(") ");
      if (record.getMessage() != null) {
        stringBuilder.append(getFormatter().formatMessage(record));
      }
      return stringBuilder.toString();
    }

    @Override
    public void publish(final LogRecord record) {
      if (isLoggable(record)) {
        try {
          if (logWriter instanceof LogWriterLogger) {
            ((LogWriterLogger) logWriter).log(record.getLevel().intValue(), getMessage(record),
                record.getThrown());
          } else {
            throw new UnsupportedOperationException(
                "LogWriterLogger (geode-dunit) does not support LogWriterImpl");
          }
        } catch (GemFireException ex) {
          reportError(null, ex, ErrorManager.WRITE_FAILURE);
        }
      }
    }
  }

  /**
   * Implementation of the standard JDK formatter that formats a message in GemFire's log format.
   */
  public static class GemFireFormatter extends Formatter {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS z");

    GemFireFormatter() {
      // nothing
    }

    @Override
    public String format(final LogRecord record) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);

      pw.println();
      pw.print('[');
      pw.print(record.getLevel().getName());
      pw.print(' ');
      pw.print(dateFormat.format(new Date(record.getMillis())));
      String threadName = Thread.currentThread().getName();
      if (threadName != null) {
        pw.print(' ');
        pw.print(threadName);
      }
      pw.print(" tid=0x");
      pw.print(Long.toHexString(Thread.currentThread().getId()));
      pw.print("] ");
      pw.print("(msgTID=");
      pw.print(record.getThreadID());

      pw.print(" msgSN=");
      pw.print(record.getSequenceNumber());
      pw.print(") ");

      String msg = record.getMessage();
      if (msg != null) {
        try {
          formatText(pw, msg, 40);
        } catch (RuntimeException e) {
          pw.println(msg);
          pw.println("Ignoring the following exception:");
          e.printStackTrace(pw);
        }
      } else {
        pw.println();
      }
      if (record.getThrown() != null) {
        record.getThrown().printStackTrace(pw);
      }
      pw.close();
      try {
        sw.close();
      } catch (IOException ignore) {
      }
      return sw.toString();
    }

    /**
     * formatText manipulates \n and \r chars but supports Windows and Linux/Unix/Mac
     */
    private static void formatText(PrintWriter writer, String target, int initialLength) {
      BreakIterator boundary = BreakIterator.getLineInstance();
      boundary.setText(target);
      int start = boundary.first();
      int end = boundary.next();
      int lineLength = initialLength;

      while (end != BreakIterator.DONE) {
        // Look at the end and only accept whitespace breaks
        char endChar = target.charAt(end - 1);
        while (!Character.isWhitespace(endChar)) {
          int lastEnd = end;
          end = boundary.next();
          if (end == BreakIterator.DONE) {
            // give up. We are at the end of the string
            end = lastEnd;
            break;
          }
          endChar = target.charAt(end - 1);
        }
        int wordEnd = end;
        if (endChar == '\n') {
          // trim off the \n since println will do it for us
          wordEnd--;
          if (wordEnd > 0 && target.charAt(wordEnd - 1) == '\r') {
            wordEnd--;
          }
        } else if (endChar == '\t') {
          // figure tabs use 8 characters
          lineLength += 7;
        }
        String word = target.substring(start, wordEnd);
        lineLength += word.length();
        writer.print(word);
        if (endChar == '\n' || endChar == '\r') {
          // force end of line
          writer.println();
          writer.print("  ");
          lineLength = 2;
        }
        start = end;
        end = boundary.next();
      }
      if (lineLength != 0) {
        writer.println();
      }
    }
  }
}
