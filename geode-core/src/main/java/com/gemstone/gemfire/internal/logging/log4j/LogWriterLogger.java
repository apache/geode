/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.logging.log4j;

import java.util.logging.Handler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.logging.GemFireHandler;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.message.GemFireParameterizedMessageFactory;

/**
 * Implements GemFireLogger with custom levels while also bridging LogWriter 
 * and LogWriterI18n to Log4J.
 * 
 */
@SuppressWarnings("unused")
public final class LogWriterLogger 
extends FastLogger 
implements InternalLogWriter, GemFireLogger {
  
  private static final long serialVersionUID = 446081244292135L;
  
  // TODO:LOG:SECURITY: need to use this either here if isSecure==true or in the security LogWriterAppender's PatternLayout but not both places
  public static final String SECURITY_PREFIX = "security-";
  
  private final ExtendedLoggerWrapper logWrapper;
  private final String connectionName;
  private final String loggerName;
  private final boolean isSecure;

  private LogWriterLogger(final Logger logger, final String connectionName, final boolean isSecure) {
    super((AbstractLogger) logger, logger.getName(), logger.getMessageFactory());
    this.logWrapper = this;
    this.connectionName = connectionName;
    this.loggerName = getName();
    this.isSecure = isSecure;
  }
  
  /**
   * Returns a custom Logger with the specified name and null connectionName.
   * 
   * @param name
   *          The this.logger name. If null the name of the calling class will
   *          be used.
   * @param isSecure
   *          True if creating a Logger for security logging.
   * @return The custom Logger.
   */
  public static LogWriterLogger create(final String name, final boolean isSecure) {
    return create(name, null, isSecure);
  }
  
  /**
   * Returns a custom Logger with the specified name.
   * 
   * @param name
   *          The this.logger name. If null the name of the calling class will
   *          be used.
   * @param connectionName
   *          The member name (also known as connection name)
   * @param isSecure
   *          True if creating a Logger for security logging.
   * @return The custom Logger.
   */
  public static LogWriterLogger create(final String name, final String connectionName, final boolean isSecure) {
    final Logger wrapped = LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE);
    return new LogWriterLogger(wrapped, connectionName, isSecure);
  }
  
  public static LogWriterLogger create(final Logger logger) {
    return new LogWriterLogger(logger, null, false);
  }
  
  public void setLevel(final Level level) {
    if (getLevel().isLessSpecificThan(Level.DEBUG) || level.isLessSpecificThan(Level.DEBUG)) {
      debug("Changing level for Logger '{}' from {} to {}", this.loggerName, getLevel(), level);
    }
    
    if (LogService.MAIN_LOGGER_NAME.equals(this.loggerName)) {
      LogService.setBaseLogLevel(level);
    } else if (LogService.SECURITY_LOGGER_NAME.equals(this.loggerName)) {
      LogService.setSecurityLogLevel(level);
    } else {
      Configurator.setLevel(this.loggerName, level);
    }
  }
  
  @Override
  public void setLogWriterLevel(final int logWriterLevel) {
    setLevel(logWriterLeveltoLog4jLevel(logWriterLevel));
  }
  
  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void finest(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finest(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finest(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finest(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finest(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void finest(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finest(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finest(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void finest(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finest(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void finest(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void finer(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finer(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finer(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finer(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finer(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void finer(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.TRACE} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finer(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finer(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void finer(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.TRACE} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finer(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.TRACE} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void finer(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.TRACE, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.DEBUG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void fine(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.DEBUG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void fine(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void fine(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void fine(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.DEBUG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void fine(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void fine(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.DEBUG} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void fine(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void fine(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.DEBUG} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void fine(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.DEBUG} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void fine(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.DEBUG} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void fine(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.DEBUG, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void config(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void config(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void config(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void config(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void config(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void config(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void config(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void config(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void config(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void config(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void config(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  @Override
  public void info(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  @Override
  public void info(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  @Override
  public void info(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void info(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  @Override
  public void info(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  @Override
  public void info(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void info(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  @Override
  public void info(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.INFO} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  @Override
  public void info(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void info(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void info(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.INFO} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void info(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.INFO} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  @Override
  public void info(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.INFO} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void info(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.INFO, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void warning(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void warning(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void warning(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of
   * the {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void warning(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void warning(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of
   * the {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void warning(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.WARN} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void warning(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void warning(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of
   * the {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.WARN} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void warning(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.WARN} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void warning(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.WARN} level including the stack trace of
   * the {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void warning(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.WARN, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.WARN} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  @Override
  public void error(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.ERROR} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  @Override
  public void error(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  @Override
  public void error(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void error(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  @Override
  public void error(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.ERROR} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  @Override
  public void error(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void error(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.ERROR} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  @Override
  public void error(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.ERROR} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  @Override
  public void error(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void error(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void error(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.ERROR} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void error(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.ERROR} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  @Override
  public void error(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.ERROR} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  @Override
  public void error(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.ERROR, null, message, t);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.FATAL} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void severe(final Marker marker, final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, msg, (Throwable) null);
  }

  /**
   * Logs a message with the specific Marker at the {@code Level.FATAL} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void severe(final Marker marker, final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void severe(final Marker marker, final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, message, t);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void severe(final Marker marker, final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.FATAL} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void severe(final Marker marker, final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, message, params);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, marker, message, t);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void severe(final Message msg) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, msg, (Throwable) null);
  }

  /**
   * Logs the specified Message at the {@code Level.FATAL} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void severe(final Message msg, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, msg, t);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void severe(final Object message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, message, (Throwable) null);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Object message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, message, t);
  }

  /**
   * Logs a message object with the {@code Level.FATAL} level.
   * 
   * @param message
   *          the message object to log.
   */
  @Override
  public void severe(final String message) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, message, (Throwable) null);
  }

  /**
   * Logs a message with parameters at the {@code Level.FATAL} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void severe(final String message, final Object... params) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, message, params);
  }

  /**
   * Logs a message at the {@code Level.FATAL} level including the stack trace of the
   * {@link Throwable} {@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack
   *          trace.LogService.getLogWriterLogger().enter()
   */
  @Override
  public void severe(final String message, final Throwable t) {
    this.logWrapper.logIfEnabled(this.loggerName, Level.FATAL, null, message, t);
  }
  
  /************************************************************
   * Methods to support backwards compatibility between levels.
   ************************************************************/
  
  public static Level logWriterLeveltoLog4jLevel(final int logWriterLevel) {
    switch (logWriterLevel) {
      case InternalLogWriter.SEVERE_LEVEL:
        return Level.FATAL;
      case InternalLogWriter.ERROR_LEVEL:
        return Level.ERROR;
      case InternalLogWriter.WARNING_LEVEL:
        return Level.WARN;
      case InternalLogWriter.CONFIG_LEVEL:
        return Level.INFO;
      case InternalLogWriter.INFO_LEVEL:
        return Level.INFO;
      case InternalLogWriter.FINE_LEVEL:
        return Level.DEBUG;
      case InternalLogWriter.FINER_LEVEL:
        return Level.DEBUG;
      case InternalLogWriter.FINEST_LEVEL:
        return Level.TRACE;
      case InternalLogWriter.ALL_LEVEL:
        return Level.ALL;
      case InternalLogWriter.NONE_LEVEL:
        return Level.OFF;
    }

    throw new IllegalArgumentException("Unknown LogWriter level [" + logWriterLevel + "].");
  }

  public static Level logWriterNametoLog4jLevel(String levelName) {
    if ("all".equalsIgnoreCase(levelName)) {
      return Level.ALL;
    }
    if ("finest".equalsIgnoreCase(levelName)) {
      return Level.TRACE;
    }
    if ("finer".equalsIgnoreCase(levelName)) {
      return Level.DEBUG;
    }
    if ("fine".equalsIgnoreCase(levelName)) {
      return Level.DEBUG;
    }
    if ("config".equalsIgnoreCase(levelName)) {
      return Level.INFO;
    }
    if ("info".equalsIgnoreCase(levelName)) {
      return Level.INFO;
    }
    if ("warning".equalsIgnoreCase(levelName)) {
      return Level.WARN;
    }
    if ("error".equalsIgnoreCase(levelName)) {
      return Level.ERROR;
    }
    if ("severe".equalsIgnoreCase(levelName)) {
      return Level.FATAL;
    }
    if ("none".equalsIgnoreCase(levelName)) {
      return Level.OFF;
    }
    
    throw new IllegalArgumentException("Unknown LogWriter level [" + levelName + "].");
  }
    
  public static int log4jLevelToLogWriterLevel(final Level log4jLevel) {
    if (log4jLevel == Level.FATAL) {
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
      return InternalLogWriter.FINEST_LEVEL;
    } else if (log4jLevel == Level.ALL) {
      return InternalLogWriter.ALL_LEVEL;
    } else if (log4jLevel == Level.OFF) {
      return InternalLogWriter.NONE_LEVEL;
    }

    throw new IllegalArgumentException("Unknown Log4J level [" + log4jLevel + "].");
  }
  
  public void log(int logWriterLevel, final String message, final Throwable t) {
    Level level = logWriterLeveltoLog4jLevel(logWriterLevel);
    this.logWrapper.logIfEnabled(this.loggerName, level, null, message, t);
  }
  
  /*****************************************
   * Methods below are specific to LogWriter
   *****************************************/
  
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
    this.finer("ENTRY {}:{}", sourceClass, sourceMethod);
  }

  @Override
  public void exiting(String sourceClass, String sourceMethod) {
    this.finer("RETURN {}:{}", sourceClass, sourceMethod);
  }

  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    this.finer("THROW {}:{}", sourceClass, sourceMethod, thrown);
  }

  @Override
  public Handler getHandler() {
    return new GemFireHandler(this); // TODO:LOG:CLEANUP: DO WE NEED A DIFFERENT HANDLER OR IS THIS OKAY?
  }

  @Override
  public LogWriterI18n convertToLogWriterI18n() {
    return this;
  }

  @Override
  public void severe(Throwable ex) {
    severe((String) null, ex);
  }

  @Override
  public void error(Throwable ex) {
    error((String) null, ex);
  }

  @Override
  public void warning(Throwable ex) {
    warning((String) null, ex);
  }

  @Override
  public void info(Throwable ex) {
    info((String) null, ex);
  }

  @Override
  public void config(Throwable ex) {
    config((String) null, ex);
  }

  @Override
  public void fine(Throwable ex) {
    fine((String) null, ex);
  }

  @Override
  public void finer(Throwable ex) {
    finer((String) null, ex);
  }

  @Override
  public void finest(Throwable ex) {
    finest((String) null, ex);
  }

  /********************************************
   * Methods below are specific to LogWriterI18n
   *********************************************/

  @Override
  public void severe(StringId msgID, Object[] params, Throwable ex) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString(params), ex);
    }
  }

  @Override
  public void severe(StringId msgID, Object param, Throwable ex) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString(param), ex);
    }
  }

  @Override
  public void severe(StringId msgID, Throwable ex) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString(), ex);
    }
  }

  @Override
  public void severe(StringId msgID, Object[] params) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString(params));
    }
  }

  @Override
  public void severe(StringId msgID, Object param) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString(param));
    }
  }

  @Override
  public void severe(StringId msgID) {
    if (isEnabled(Level.FATAL)) {
      severe(msgID.toLocalizedString());
    }
  }

  @Override
  public void error(StringId msgID, Object[] params, Throwable ex) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString(params), ex);
    }
  }

  @Override
  public void error(StringId msgID, Object param, Throwable ex) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString(param), ex);
    }
  }

  @Override
  public void error(StringId msgID, Throwable ex) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString(), ex);
    }
  }

  @Override
  public void error(StringId msgID, Object[] params) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString(params));
    }
  }

  @Override
  public void error(StringId msgID, Object param) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString(param));
    }
  }

  @Override
  public void error(StringId msgID) {
    if (isEnabled(Level.ERROR)) {
      error(msgID.toLocalizedString());
    }
  }

  @Override
  public void warning(StringId msgID, Object[] params, Throwable ex) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString(params), ex);
    }
  }

  @Override
  public void warning(StringId msgID, Object param, Throwable ex) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString(param), ex);
    }
  }

  @Override
  public void warning(StringId msgID, Throwable ex) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString(), ex);
    }
  }

  @Override
  public void warning(StringId msgID, Object[] params) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString(params));
    }
  }

  @Override
  public void warning(StringId msgID, Object param) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString(param));
    }
  }

  @Override
  public void warning(StringId msgID) {
    if (isEnabled(Level.WARN)) {
      warning(msgID.toLocalizedString());
    }
  }

  @Override
  public void info(StringId msgID, Object[] params, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString(params), ex);
    }
  }

  @Override
  public void info(StringId msgID, Object param, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString(param), ex);
    }
  }

  @Override
  public void info(StringId msgID, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString(), ex);
    }
  }

  @Override
  public void info(StringId msgID, Object[] params) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString(params));
    }
  }

  @Override
  public void info(StringId msgID, Object param) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString(param));
    }
  }

  @Override
  public void info(StringId msgID) {
    if (isEnabled(Level.INFO)) {
      info(msgID.toLocalizedString());
    }
  }

  @Override
  public void config(StringId msgID, Object[] params, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString(params), ex);
    }
  }

  @Override
  public void config(StringId msgID, Object param, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString(param), ex);
    }
  }

  @Override
  public void config(StringId msgID, Throwable ex) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString(), ex);
    }
  }

  @Override
  public void config(StringId msgID, Object[] params) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString(params));
    }
  }

  @Override
  public void config(StringId msgID, Object param) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString(param));
    }
  }

  @Override
  public void config(StringId msgID) {
    if (isEnabled(Level.INFO)) {
      config(msgID.toLocalizedString());
    }
  }

  @Override
  public LogWriter convertToLogWriter() {
    return this;
  }

  @Override
  public int getLogWriterLevel() {
    final Level log4jLevel = this.logWrapper.getLevel();
    
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
    
    throw new IllegalStateException("Level " + log4jLevel + " could not be mapped to LogWriter level.");
  }

  @Override
  public boolean isSecure() {
    return this.isSecure;
  }

  @Override
  public void put(int msgLevel, String msg, Throwable exception) {
    log(msgLevel, msg, exception);
  }

  @Override
  public void put(int msgLevel, StringId msgId, Object[] params, Throwable exception) {
    log(msgLevel, msgId.toLocalizedString(params), exception);
  }
  
  @Override
  public String getConnectionName() {
    return this.connectionName;
  }
}
