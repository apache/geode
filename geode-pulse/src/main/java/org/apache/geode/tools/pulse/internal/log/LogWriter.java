/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.log;

/**
 * interface LogWriter
 * 
 * LogWriter interface for Pulse Logging.
 * 
 * @since GemFire 7.0.1
 * 
 */
public interface LogWriter {
  /**
   * Returns true if "severe" log messages are enabled. Returns false if
   * "severe" log messages are disabled.
   */
  public boolean severeEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "severe".
   */
  public void severe(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "severe".
   */
  public void severe(String msg);

  /**
   * Writes an exception to this writer. The exception level is "severe".
   */
  public void severe(Throwable ex);

  /**
   * Returns true if "error" log messages are enabled. Returns false if "error"
   * log messages are disabled.
   */
  // public boolean errorEnabled();
  /**
   * Writes both a message and exception to this writer. The message level is
   * "error".
   */
  // public void error(String msg, Throwable ex);
  /**
   * Writes a message to this writer. The message level is "error".
   */
  // public void error(String msg);
  /**
   * Writes an exception to this writer. The exception level is "error".
   */
  // public void error(Throwable ex);
  /**
   * Returns true if "warning" log messages are enabled. Returns false if
   * "warning" log messages are disabled.
   */
  public boolean warningEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "warning".
   */
  public void warning(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "warning".
   */
  public void warning(String msg);

  /**
   * Writes an exception to this writer. The exception level is "warning".
   */
  public void warning(Throwable ex);

  /**
   * Returns true if "info" log messages are enabled. Returns false if "info"
   * log messages are disabled.
   */
  public boolean infoEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "information".
   */
  public void info(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "information".
   */
  public void info(String msg);

  /**
   * Writes an exception to this writer. The exception level is "information".
   */
  public void info(Throwable ex);

  /**
   * Returns true if "config" log messages are enabled. Returns false if
   * "config" log messages are disabled.
   */
  public boolean configEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "config".
   */
  public void config(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "config".
   */
  public void config(String msg);

  /**
   * Writes an exception to this writer. The exception level is "config".
   */
  public void config(Throwable ex);

  /**
   * Returns true if "fine" log messages are enabled. Returns false if "fine"
   * log messages are disabled.
   */
  public boolean fineEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "fine".
   */
  public void fine(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "fine".
   */
  public void fine(String msg);

  /**
   * Writes an exception to this writer. The exception level is "fine".
   */
  public void fine(Throwable ex);

  /**
   * Returns true if "finer" log messages are enabled. Returns false if "finer"
   * log messages are disabled.
   */
  public boolean finerEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "finer".
   */
  public void finer(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "finer".
   */
  public void finer(String msg);

  /**
   * Writes an exception to this writer. The exception level is "finer".
   */
  public void finer(Throwable ex);

  /**
   * Log a method entry.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message
   * will start with <code>"ENTRY"</code> and include the class and method
   * names.
   * 
   * @param sourceClass
   *          Name of class that issued the logging request.
   * @param sourceMethod
   *          Name of the method that issued the logging request.
   */
  public void entering(String sourceClass, String sourceMethod);

  /**
   * Log a method return.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message
   * will start with <code>"RETURN"</code> and include the class and method
   * names.
   * 
   * @param sourceClass
   *          Name of class that issued the logging request.
   * @param sourceMethod
   *          Name of the method that issued the logging request.
   */
  public void exiting(String sourceClass, String sourceMethod);

  /**
   * Log throwing an exception.
   * <p>
   * Use to log that a method is terminating by throwing an exception. The
   * logging is done using the <code>finer</code> level.
   * <p>
   * This is a convenience method that could be done instead by calling
   * {@link #finer(String, Throwable)}. The string message will start with
   * <code>"THROW"</code> and include the class and method names.
   * 
   * @param sourceClass
   *          Name of class that issued the logging request.
   * @param sourceMethod
   *          Name of the method that issued the logging request.
   * @param thrown
   *          The Throwable that is being thrown.
   */
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown);

  /**
   * Returns true if "finest" log messages are enabled. Returns false if
   * "finest" log messages are disabled.
   */
  public boolean finestEnabled();

  /**
   * Writes both a message and exception to this writer. The message level is
   * "finest".
   */
  public void finest(String msg, Throwable ex);

  /**
   * Writes a message to this writer. The message level is "finest".
   */
  public void finest(String msg);

  /**
   * Writes an exception to this writer. The exception level is "finest".
   */
  public void finest(Throwable ex);

  /**
   * Returns a 1.4 logging handler that can be used to direct application output
   * to this GemFire logger using the standard JDK logger APIs. Each time this
   * method is called it creates a new instance of a Handler so care should be
   * taken to not call this method too often.
   */
  // public Handler getHandler();

  /**
   * A mechanism for accessing the abstraction layer used for
   * internationalization.
   * 
   * @return LogWriterI18n
   */
  // public LogWriterI18n convertToLogWriterI18n();
}