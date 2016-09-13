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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Extends Logger interface with convenience methods for the FINEST, FINER,
 * FINE, CONFIG, INFO, WARNING, ERROR and SEVERE custom log levels.
 * 
 */
public interface GemFireLogger extends Logger {

  /**
   * Logs a message with the specific Marker at the;@code FINEST} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void finest(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code FINEST} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finest(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINEST} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finest(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code FINEST} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINEST} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finest(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code FINEST} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finest(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code FINEST} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code FINEST} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void finest(final Message msg);

  /**
   * Logs the specified Message at the;@code FINEST} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finest(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINEST} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finest(final Object message);

  /**
   * Logs a message at the;@code FINEST} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINEST} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finest(final String message);

  /**
   * Logs a message with parameters at the;@code FINEST} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finest(final String message, final Object... params);

  /**
   * Logs a message at the;@code FINEST} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finest(final String message, final Throwable t);

  /**
   * Logs a message with the specific Marker at the;@code FINER} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void finer(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code FINER} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finer(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINER} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finer(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code FINER} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINER} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void finer(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code FINER} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finer(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code FINER} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code FINER} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void finer(final Message msg);

  /**
   * Logs the specified Message at the;@code FINER} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void finer(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINER} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finer(final Object message);

  /**
   * Logs a message at the;@code FINER} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINER} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void finer(final String message);

  /**
   * Logs a message with parameters at the;@code FINER} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void finer(final String message, final Object... params);

  /**
   * Logs a message at the;@code FINER} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void finer(final String message, final Throwable t);

  /**
   * Logs a message with the specific Marker at the;@code FINE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void fine(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code FINE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void fine(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void fine(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code FINE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void fine(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code FINE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void fine(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code FINE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code FINE} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void fine(final Message msg);

  /**
   * Logs the specified Message at the;@code FINE} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void fine(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code FINE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void fine(final Object message);

  /**
   * Logs a message at the;@code FINE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code FINE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void fine(final String message);

  /**
   * Logs a message with parameters at the;@code FINE} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void fine(final String message, final Object... params);

  /**
   * Logs a message at the;@code FINE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fine(final String message, final Throwable t);

  /**
   * Logs a message with the specific Marker at the;@code CONFIG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void config(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code CONFIG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void config(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code CONFIG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void config(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code CONFIG} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code CONFIG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void config(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code CONFIG} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void config(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code CONFIG} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code CONFIG} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void config(final Message msg);

  /**
   * Logs the specified Message at the;@code CONFIG} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void config(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code CONFIG} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void config(final Object message);

  /**
   * Logs a message at the;@code CONFIG} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code CONFIG} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void config(final String message);

  /**
   * Logs a message with parameters at the;@code CONFIG} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void config(final String message, final Object... params);

  /**
   * Logs a message at the;@code CONFIG} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void config(final String message, final Throwable t);

  /**
   * Logs a message with the specific Marker at the;@code WARNING} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void warning(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code WARNING} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void warning(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code WARNING} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void warning(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code WARNING} level including the stack trace of
   * the;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code WARNING} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void warning(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code WARNING} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void warning(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code WARNING} level including the stack trace of
   * the;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code WARNING} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void warning(final Message msg);

  /**
   * Logs the specified Message at the;@code WARNING} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void warning(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code WARNING} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void warning(final Object message);

  /**
   * Logs a message at the;@code WARNING} level including the stack trace of
   * the;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code WARNING} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void warning(final String message);

  /**
   * Logs a message with parameters at the;@code WARNING} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void warning(final String message, final Object... params);

  /**
   * Logs a message at the;@code WARNING} level including the stack trace of
   * the;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warning(final String message, final Throwable t);

  /**
   * Logs a message with the specific Marker at the;@code SEVERE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   */
  public void severe(final Marker marker, final Message msg);

  /**
   * Logs a message with the specific Marker at the;@code SEVERE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void severe(final Marker marker, final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code SEVERE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void severe(final Marker marker, final Object message);

  /**
   * Logs a message at the;@code SEVERE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code SEVERE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message object to log.
   */
  public void severe(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the;@code SEVERE} level.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void severe(final Marker marker, final String message, final Object... params);

  /**
   * Logs a message at the;@code SEVERE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param marker
   *          the marker data specific to this log statement
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Marker marker, final String message, final Throwable t);

  /**
   * Logs the specified Message at the;@code SEVERE} level.
   * 
   * @param msg
   *          the message string to be logged
   */
  public void severe(final Message msg);

  /**
   * Logs the specified Message at the;@code SEVERE} level.
   * 
   * @param msg
   *          the message string to be logged
   * @param t
   *          A Throwable or null.
   */
  public void severe(final Message msg, final Throwable t);

  /**
   * Logs a message object with the;@code SEVERE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void severe(final Object message);

  /**
   * Logs a message at the;@code SEVERE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void severe(final Object message, final Throwable t);

  /**
   * Logs a message object with the;@code SEVERE} level.
   * 
   * @param message
   *          the message object to log.
   */
  public void severe(final String message);

  /**
   * Logs a message with parameters at the;@code SEVERE} level.
   * 
   * @param message
   *          the message to log; the format depends on the message factory.
   * @param params
   *          parameters to the message.
   * @see #getMessageFactory()
   */
  public void severe(final String message, final Object... params);

  /**
   * Logs a message at the;@code SEVERE} level including the stack trace of the
   *;@link Throwable};@code t} passed as parameter.
   * 
   * @param message
   *          the message to log.
   * @param t
   *          the exception to log, including its stack
   *          trace.LogService.getLogWriterLogger().enter()
   */
  public void severe(final String message, final Throwable t);

}
