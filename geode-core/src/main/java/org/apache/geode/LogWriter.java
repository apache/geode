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
package org.apache.geode;

import java.util.logging.Handler;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.i18n.LogWriterI18n;

/**
 * Defines methods available to clients that want to write a log message to their GemFire
 * distributed system log file.
 * <p>
 * Instances of this interface can be obtained by calling {@link DistributedSystem#getLogWriter}.
 * <p>
 * For any logged message the log file will contain:
 * <ul>
 * <li>The message's level.
 * <li>The time the message was logged.
 * <li>The id of the thread that logged the message.
 * <li>The message itself which can be a string and/or an exception including the exception's stack
 * trace.
 * </ul>
 * <p>
 * A message always has a level. Logging levels are ordered. Enabling logging at a given level also
 * enables logging at higher levels. The higher the level the more important and urgent the message.
 * <p>
 * The levels, in descending order, are:
 * <ul>
 * <li><code>severe</code> (highest value) is a message level indicating a serious failure. In
 * general <code>severe</code> messages should describe events that are of considerable importance
 * and which will prevent normal program execution. They should be reasonably intelligible to end
 * users and to information managers.
 * <li><code>error</code> In general <code>error</code> messages should describe events that are of
 * considerable importance but will not prevent normal program execution. They should be reasonably
 * intelligible to end users and to information managers. They are weaker than <code>severe</code>
 * and stronger than <code>warning</code>.
 * <li><code>warning</code> is a message level indicating a potential problem. In general
 * <code>warning</code> messages should describe events that will be of interest to end users or
 * information managers, or which indicate potential problems.
 * <li><code>info</code> is a message level for informational messages. Typically <code>info</code>
 * messages should be reasonably significant and should make sense to end users and system
 * administrators.
 * <li><code>config</code> is a message level for static configuration messages. <code>config</code>
 * messages are intended to provide a variety of static configuration information, to assist in
 * debugging problems that may be associated with particular configurations.
 * <li><code>fine</code> is a message level providing tracing information. In general the
 * <code>fine</code> level should be used for information that will be broadly interesting to
 * developers. This level is for the lowest volume, and most important, tracing messages.
 * <li><code>finer</code> indicates a fairly detailed tracing message. Logging calls for entering,
 * returning, or throwing an exception are traced at the <code>finer</code> level.
 * <li><code>finest</code> (lowest value) indicates a highly detailed tracing message. In general
 * the <code>finest</code> level should be used for the most voluminous detailed tracing messages.
 * </ul>
 * <p>
 * For each level methods exist that will request a message, at that level, to be logged. These
 * methods are all named after their level.
 * <p>
 * For each level a method exists that returns a boolean indicating if messages at that level will
 * currently be logged. The names of these methods are of the form:
 * <em>level</em><code>Enabled</code>.
 *
 * @deprecated Please use Log4J2 instead.
 */
@Deprecated
public interface LogWriter {

  /**
   * Returns true if "severe" log messages are enabled. Returns false if "severe" log messages are
   * disabled.
   */
  boolean severeEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   */
  void severe(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "severe".
   */
  void severe(String message);

  /**
   * Writes an exception to this writer. The exception level is "severe".
   */
  void severe(Throwable throwable);

  /**
   * Returns true if "error" log messages are enabled. Returns false if "error" log messages are
   * disabled.
   */
  boolean errorEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   */
  void error(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "error".
   */
  void error(String message);

  /**
   * Writes an exception to this writer. The exception level is "error".
   */
  void error(Throwable throwable);

  /**
   * Returns true if "warning" log messages are enabled. Returns false if "warning" log messages are
   * disabled.
   */
  boolean warningEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   */
  void warning(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "warning".
   */
  void warning(String message);

  /**
   * Writes an exception to this writer. The exception level is "warning".
   */
  void warning(Throwable throwable);

  /**
   * Returns true if "info" log messages are enabled. Returns false if "info" log messages are
   * disabled.
   */
  boolean infoEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "information".
   */
  void info(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "information".
   */
  void info(String message);

  /**
   * Writes an exception to this writer. The exception level is "information".
   */
  void info(Throwable throwable);

  /**
   * Returns true if "config" log messages are enabled. Returns false if "config" log messages are
   * disabled.
   */
  boolean configEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   */
  void config(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "config".
   */
  void config(String message);

  /**
   * Writes an exception to this writer. The exception level is "config".
   */
  void config(Throwable throwable);

  /**
   * Returns true if "fine" log messages are enabled. Returns false if "fine" log messages are
   * disabled.
   */
  boolean fineEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "fine".
   */
  void fine(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "fine".
   */
  void fine(String message);

  /**
   * Writes an exception to this writer. The exception level is "fine".
   */
  void fine(Throwable throwable);

  /**
   * Returns true if "finer" log messages are enabled. Returns false if "finer" log messages are
   * disabled.
   */
  boolean finerEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "finer".
   */
  void finer(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "finer".
   */
  void finer(String message);

  /**
   * Writes an exception to this writer. The exception level is "finer".
   */
  void finer(Throwable throwable);

  /**
   * Log a method entry.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message will start with
   * <code>"ENTRY"</code> and include the class and method names.
   *
   * @param sourceClass Name of class that issued the logging request.
   * @param sourceMethod Name of the method that issued the logging request.
   */
  void entering(String sourceClass, String sourceMethod);

  /**
   * Log a method return.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message will start with
   * <code>"RETURN"</code> and include the class and method names.
   *
   * @param sourceClass Name of class that issued the logging request.
   * @param sourceMethod Name of the method that issued the logging request.
   */
  void exiting(String sourceClass, String sourceMethod);

  /**
   * Log throwing an exception.
   * <p>
   * Use to log that a method is terminating by throwing an exception. The logging is done using the
   * <code>finer</code> level.
   * <p>
   * This is a convenience method that could be done instead by calling
   * {@link #finer(String, Throwable)}. The string message will start with <code>"THROW"</code> and
   * include the class and method names.
   *
   * @param sourceClass Name of class that issued the logging request.
   * @param sourceMethod Name of the method that issued the logging request.
   * @param thrown The Throwable that is being thrown.
   */
  void throwing(String sourceClass, String sourceMethod, Throwable thrown);

  /**
   * Returns true if "finest" log messages are enabled. Returns false if "finest" log messages are
   * disabled.
   */
  boolean finestEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is "finest".
   */
  void finest(String message, Throwable throwable);

  /**
   * Writes a message to this writer. The message level is "finest".
   */
  void finest(String message);

  /**
   * Writes an exception to this writer. The exception level is "finest".
   */
  void finest(Throwable throwable);

  /**
   * Returns a 1.4 logging handler that can be used to direct application output to this GemFire
   * logger using the standard JDK logger APIs. Each time this method is called it creates a new
   * instance of a Handler so care should be taken to not call this method too often.
   */
  Handler getHandler();

  /**
   * A mechanism for accessing the abstraction layer used for internationalization.
   */
  LogWriterI18n convertToLogWriterI18n();
}
