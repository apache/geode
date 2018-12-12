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
package org.apache.geode.internal.logging.log4j;

import java.util.logging.Handler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.logging.GemFireHandler;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessageFactory;

/**
 * Implements GemFireLogger with custom levels while also bridging LogWriter and LogWriterI18n to
 * Log4J.
 */
@SuppressWarnings("unused")
public class LogWriterLogger extends FastLogger implements InternalLogWriter, GemFireLogger {

  private static final long serialVersionUID = 446081244292135L;

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
    Logger wrapped = LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE);
    return new LogWriterLogger(wrapped, connectionName, isSecure);
  }

  public static LogWriterLogger create(final Logger logger) {
    return new LogWriterLogger(logger, null, false);
  }

  public void setLevel(final Level level) {
    if (getLevel().isLessSpecificThan(Level.DEBUG) || level.isLessSpecificThan(Level.DEBUG)) {
      debug("Changing level for Logger '{}' from {} to {}", loggerName, getLevel(), level);
    }

    if (LogService.MAIN_LOGGER_NAME.equals(loggerName)) {
      LogService.setBaseLogLevel(level);
    } else if (LogService.SECURITY_LOGGER_NAME.equals(loggerName)) {
      LogService.setSecurityLogLevel(level);
    } else {
      Configurator.setLevel(loggerName, level);
    }
  }

  @Override
  public void setLogWriterLevel(final int logWriterLevel) {
    setLevel(LogLevel.getLog4jLevel(logWriterLevel));
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  @Override
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
  @Override
  public void finest(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void finest(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void finest(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void finest(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void finest(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public void finer(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void finer(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void finer(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void finer(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void finer(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.TRACE, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void finer(final String message, final Object... parameters) {
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
  @Override
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
  @Override
  public void fine(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void fine(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void fine(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void fine(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void fine(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.DEBUG, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public void config(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void config(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void config(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void config(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void config(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.INFO, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public void warning(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void warning(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void warning(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void warning(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void warning(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.WARN, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public void severe(final Marker marker, final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
  public void severe(final Marker marker, final Object message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
  public void severe(final Marker marker, final String message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, marker, message, throwable);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   *
   * @param message the message string to be logged
   */
  @Override
  public void severe(final Message message) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, null);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  @Override
  public void severe(final Message message, final Throwable throwable) {
    logWrapper.logIfEnabled(loggerName, Level.FATAL, null, message, throwable);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   *
   * @param message the message object to log.
   */
  @Override
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
  @Override
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
  @Override
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
    Level level = LogLevel.getLog4jLevel(logWriterLevel);
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

  @Override
  public int getLogWriterLevel() {
    final Level log4jLevel = logWrapper.getLevel();

    if (log4jLevel == Level.OFF) {
      return InternalLogWriter.NONE_LEVEL;
    } else if (log4jLevel == Level.FATAL) {
      return InternalLogWriter.SEVERE_LEVEL;
    } else if (log4jLevel == Level.ERROR) {
      return InternalLogWriter.ERROR_LEVEL;
    } else if (log4jLevel == Level.WARN) {
      return InternalLogWriter.WARNING_LEVEL;
    } else if (log4jLevel == Level.INFO) {
      return InternalLogWriter.INFO_LEVEL;
    } else if (log4jLevel == Level.DEBUG) {
      return InternalLogWriter.FINE_LEVEL;
    } else if (log4jLevel == Level.TRACE) {
      return InternalLogWriter.FINER_LEVEL;
    } else if (log4jLevel == Level.ALL) {
      return InternalLogWriter.ALL_LEVEL;
    }

    throw new IllegalStateException(
        "Level " + log4jLevel + " could not be mapped to LogWriter level.");
  }

  @Override
  public boolean isSecure() {
    return isSecure;
  }

  @Override
  public void put(int messageLevel, String message, Throwable throwable) {
    log(messageLevel, message, throwable);
  }

  @Override
  public void put(int messageLevel, StringId messageId, Object[] parameters, Throwable throwable) {
    log(messageLevel, messageId.toLocalizedString(parameters), throwable);
  }

  @Override
  public String getConnectionName() {
    return connectionName;
  }
}
