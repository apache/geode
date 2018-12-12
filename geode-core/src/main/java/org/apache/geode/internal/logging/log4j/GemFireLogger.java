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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;

/**
 * Extends Logger interface with convenience methods for the FINEST, FINER, FINE, CONFIG, INFO,
 * WARNING, ERROR and SEVERE custom log levels.
 */
public interface GemFireLogger extends Logger {

  /**
   * Logs a message with the specific Marker at the {@code FINEST} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void finest(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code FINEST} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void finest(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINEST} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void finest(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code FINEST} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finest(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINEST} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void finest(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code FINEST} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void finest(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINEST} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finest(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code FINEST} level.
   *
   * @param message the message string to be logged
   */
  void finest(final Message message);

  /**
   * Logs the specified Message at the {@code FINEST} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void finest(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINEST} level.
   *
   * @param message the message object to log.
   */
  void finest(final Object message);

  /**
   * Logs a message at the {@code FINEST} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finest(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINEST} level.
   *
   * @param message the message object to log.
   */
  void finest(final String message);

  /**
   * Logs a message with parameters at the {@code FINEST} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   * @see #getMessageFactory()
   */
  void finest(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINEST} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finest(final String message, final Throwable throwable);

  /**
   * Logs a message with the specific Marker at the {@code FINER} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void finer(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code FINER} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void finer(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINER} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void finer(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code FINER} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finer(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINER} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void finer(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code FINER} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void finer(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINER} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finer(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code FINER} level.
   *
   * @param message the message string to be logged
   */
  void finer(final Message message);

  /**
   * Logs the specified Message at the {@code FINER} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void finer(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINER} level.
   *
   * @param message the message object to log.
   */
  void finer(final Object message);

  /**
   * Logs a message at the {@code FINER} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finer(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINER} level.
   *
   * @param message the message object to log.
   */
  void finer(final String message);

  /**
   * Logs a message with parameters at the {@code FINER} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void finer(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINER} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void finer(final String message, final Throwable throwable);

  /**
   * Logs a message with the specific Marker at the {@code FINE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void fine(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code FINE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void fine(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void fine(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code FINE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void fine(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void fine(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code FINE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void fine(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void fine(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code FINE} level.
   *
   * @param message the message string to be logged
   */
  void fine(final Message message);

  /**
   * Logs the specified Message at the {@code FINE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void fine(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINE} level.
   *
   * @param message the message object to log.
   */
  void fine(final Object message);

  /**
   * Logs a message at the {@code FINE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void fine(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code FINE} level.
   *
   * @param message the message object to log.
   */
  void fine(final String message);

  /**
   * Logs a message with parameters at the {@code FINE} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void fine(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code FINE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void fine(final String message, final Throwable throwable);

  /**
   * Logs a message with the specific Marker at the {@code CONFIG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void config(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code CONFIG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void config(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code CONFIG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void config(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code CONFIG} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void config(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code CONFIG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void config(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code CONFIG} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void config(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code CONFIG} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void config(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code CONFIG} level.
   *
   * @param message the message string to be logged
   */
  void config(final Message message);

  /**
   * Logs the specified Message at the {@code CONFIG} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void config(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code CONFIG} level.
   *
   * @param message the message object to log.
   */
  void config(final Object message);

  /**
   * Logs a message at the {@code CONFIG} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void config(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code CONFIG} level.
   *
   * @param message the message object to log.
   */
  void config(final String message);

  /**
   * Logs a message with parameters at the {@code CONFIG} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void config(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code CONFIG} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void config(final String message, final Throwable throwable);

  /**
   * Logs a message with the specific Marker at the {@code WARNING} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void warning(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code WARNING} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void warning(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code WARNING} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void warning(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code WARNING} level including the stack trace of the{@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void warning(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code WARNING} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void warning(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code WARNING} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void warning(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code WARNING} level including the stack trace of the{@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void warning(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code WARNING} level.
   *
   * @param message the message string to be logged
   */
  void warning(final Message message);

  /**
   * Logs the specified Message at the {@code WARNING} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void warning(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code WARNING} level.
   *
   * @param message the message object to log.
   */
  void warning(final Object message);

  /**
   * Logs a message at the {@code WARNING} level including the stack trace of the{@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void warning(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code WARNING} level.
   *
   * @param message the message object to log.
   */
  void warning(final String message);

  /**
   * Logs a message with parameters at the {@code WARNING} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void warning(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code WARNING} level including the stack trace of the{@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void warning(final String message, final Throwable throwable);

  /**
   * Logs a message with the specific Marker at the {@code SEVERE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   */
  void severe(final Marker marker, final Message message);

  /**
   * Logs a message with the specific Marker at the {@code SEVERE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void severe(final Marker marker, final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code SEVERE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void severe(final Marker marker, final Object message);

  /**
   * Logs a message at the {@code SEVERE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void severe(final Marker marker, final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code SEVERE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message object to log.
   */
  void severe(final Marker marker, final String message);

  /**
   * Logs a message with parameters at the {@code SEVERE} level.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void severe(final Marker marker, final String message, final Object... parameters);

  /**
   * Logs a message at the {@code SEVERE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param marker the marker data specific to this log statement
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void severe(final Marker marker, final String message, final Throwable throwable);

  /**
   * Logs the specified Message at the {@code SEVERE} level.
   *
   * @param message the message string to be logged
   */
  void severe(final Message message);

  /**
   * Logs the specified Message at the {@code SEVERE} level.
   *
   * @param message the message string to be logged
   * @param throwable A Throwable or null.
   */
  void severe(final Message message, final Throwable throwable);

  /**
   * Logs a message object with the {@code SEVERE} level.
   *
   * @param message the message object to log.
   */
  void severe(final Object message);

  /**
   * Logs a message at the {@code SEVERE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack trace.
   */
  void severe(final Object message, final Throwable throwable);

  /**
   * Logs a message object with the {@code SEVERE} level.
   *
   * @param message the message object to log.
   */
  void severe(final String message);

  /**
   * Logs a message with parameters at the {@code SEVERE} level.
   *
   * @param message the message to log; the format depends on the message factory.
   * @param parameters parameters to the message.
   */
  void severe(final String message, final Object... parameters);

  /**
   * Logs a message at the {@code SEVERE} level including the stack trace of the {@link
   * Throwable} {@code throwable} passed as parameter.
   *
   * @param message the message to log.
   * @param throwable the exception to log, including its stack
   *        trace.LogService.getLogWriterLogger().enter()
   */
  void severe(final String message, final Throwable throwable);

}
