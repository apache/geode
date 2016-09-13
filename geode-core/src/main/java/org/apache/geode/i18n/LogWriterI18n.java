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

package com.gemstone.gemfire.i18n;

import java.util.logging.Handler;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
/**
  * Defines methods available to clients that want to write a log message
  * to their GemFire distributed system log file.
  * <p>
  * Instances of this interface can be obtained by calling
  * {@link DistributedSystem#getLogWriter}.
  * <p>
  * For any logged message the log file will contain:
  * <ul>
  * <li> The message's level.
  * <li> The time the message was logged.
  * <li> The id of the thread that logged the message.
  * <li> The message itself which can be a string and/or an exception
  *      including the exception's stack trace.
  * </ul>
  * <p>
  * A message always has a level.
  * Logging levels are ordered. Enabling logging at a given level also
  * enables logging at higher levels. The higher the level the more
  * important and urgent the message.
  * <p>
  * The levels, in descending order, are:
  * <ul>
  * <li> <code>severe</code>  (highest value) is a message level indicating a serious failure.
  *   In general <code>severe</code> messages should describe events that
  *   are of considerable importance and which will prevent normal program
  *   execution. They should be reasonably intelligible to end users and
  *   to information managers.
  * <li> <code>error</code>  
  *   In general <code>error</code> messages should describe events that
  *   are of considerable importance but will not prevent normal program
  *   execution. They should be reasonably intelligible to end users and
  *   to information managers. They are weaker than <code>severe</code> and
  *   stronger than <code>warning</code>.
  * <li> <code>warning</code> is a message level indicating a potential problem.
  *   In general <code>warning</code> messages should describe events that
  *   will be of interest to end users or information managers, or which indicate
  *   potential problems.
  * <li> <code>info</code> is a message level for informational messages.
  *   Typically <code>info</code> messages should be reasonably significant
  *   and should make sense to end users and system administrators.
  * <li> <code>config</code> is a message level for static configuration messages.
  *   <code>config</code> messages are intended to provide a variety of static
  *   configuration information, to assist in debugging problems that may be
  *   associated with particular configurations.
  * <li> <code>fine</code> is a message level providing tracing information.
  *   In general the <code>fine</code> level should be used for information
  *   that will be broadly interesting to developers. This level is for
  *   the lowest volume, and most important, tracing messages.
  * <li> <code>finer</code> indicates a fairly detailed tracing message.
  *   Logging calls for entering, returning, or throwing an exception
  *   are traced at the <code>finer</code> level.
  * <li> <code>finest</code> (lowest value) indicates a highly detailed tracing message.
  *   In general the <code>finest</code> level should be used for the most
  *   voluminous detailed tracing messages.
  * </ul>
  * <p>
  * For each level methods exist that will request a message, at that
  * level, to be logged. These methods are all named after their level.
  * <p>
  * For each level a method exists that returns a boolean indicating
  * if messages at that level will currently be logged. The names
  * of these methods are of the form: <em>level</em><code>Enabled</code>.
  *
  */
public interface LogWriterI18n {
    /**
     * @return true if "severe" log messages are enabled.
     */
    public boolean severeEnabled();
    /**
     * Writes an exception to this writer.
     * The exception level is "severe".
     */
    public void severe(Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID, Object[] params, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID, Object param, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID, Object[] params);
    /**
     * Writes a message to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID, Object param);
    /**
     * Writes a message to this writer.
     * The message level is "severe".
     * @since GemFire 6.0
     */
    public void severe(StringId msgID);
    /**
     * @return true if "error" log messages are enabled.
     */
    public boolean errorEnabled();
    /**
     * Writes an exception to this writer.
     * The exception level is "error".
     */
    public void error(Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID, Object[] params, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID, Object param, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID, Object[] params);
    /**
     * Writes a message to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID, Object param);
    /**
     * Writes a message to this writer.
     * The message level is "error".
     * @since GemFire 6.0
     */
    public void error(StringId msgID);
    /**
     * @return true if "warning" log messages are enabled.
     */
    public boolean warningEnabled();
    /**
     * Writes an exception to this writer.
     * The exception level is "warning".
     */
    public void warning(Throwable ex);
     /**
     * Writes both a message and exception to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID, Object[] params, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID, Object param, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID, Object[] params);
    /**
     * Writes a message to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID, Object param);
    /**
     * Writes a message to this writer.
     * The message level is "warning".
     * @since GemFire 6.0
     */
    public void warning(StringId msgID);
    /**
     * @return true if "info" log messages are enabled.
     */
    public boolean infoEnabled(); 
    /**
     * Writes an exception to this writer.
     * The exception level is "information".
     */
    public void info(Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID, Object[] params, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID, Object param, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID, Object[] params);
    /**
     * Writes a message to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID, Object param);
    /**
     * Writes a message to this writer.
     * The message level is "info".
     * @since GemFire 6.0
     */
    public void info(StringId msgID);
    /**
     * @return true if "config" log messages are enabled.
     */
    public boolean configEnabled();
    /**
     * Writes an exception to this writer.
     * The exception level is "config".
     */
    public void config(Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID, Object[] params, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID, Object param, Throwable ex);
    /**
     * Writes both a message and exception to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID, Object[] params);
    /**
     * Writes a message to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID, Object param);
    /**
     * Writes a message to this writer.
     * The message level is "config".
     * @since GemFire 6.0
     */
    public void config(StringId msgID);
    /**
     * @return true if "fine" log messages are enabled.
     */
    public boolean fineEnabled();
    /**
     * Writes both a message and exception to this writer.
     * The message level is "fine".
     */
    public void fine(String msg, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "fine".
     */
    public void fine(String msg);
    /**
     * Writes an exception to this writer.
     * The exception level is "fine".
     */
    public void fine(Throwable ex);
    /**
     * @return true if "finer" log messages are enabled.
     */
    public boolean finerEnabled();
    /**
     * Writes both a message and exception to this writer.
     * The message level is "finer".
     */
    public void finer(String msg, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "finer".
     */
    public void finer(String msg);
    /**
     * Writes an exception to this writer.
     * The exception level is "finer".
     */
    public void finer(Throwable ex);
    /**
     * Log a method entry.
     * <p>The logging is done using the <code>finer</code> level.
     * The string message will start with <code>"ENTRY"</code> and
     * include the class and method names.
     * @param sourceClass Name of class that issued the logging request.
     * @param sourceMethod Name of the method that issued the logging request.
     */
    public void entering(String sourceClass, String sourceMethod);
    /**
     * Log a method return.
     * <p>The logging is done using the <code>finer</code> level.
     * The string message will start with <code>"RETURN"</code> and
     * include the class and method names.
     * @param sourceClass Name of class that issued the logging request.
     * @param sourceMethod Name of the method that issued the logging request.
     */
    public void exiting(String sourceClass, String sourceMethod);
    /**
     * Log throwing an exception.
     * <p> Use to log that a method is
     * terminating by throwing an exception. The logging is done using
     * the <code>finer</code> level.
     * <p> This is a convenience method that could be done
     * instead by calling {@link #finer(String, Throwable)}.
     * The string message will start with <code>"THROW"</code> and
     * include the class and method names.
     * @param sourceClass Name of class that issued the logging request.
     * @param sourceMethod Name of the method that issued the logging request.
     * @param thrown The Throwable that is being thrown.
     */
    public void throwing(String sourceClass, String sourceMethod,
			 Throwable thrown);
    /**
     * @return true if "finest" log messages are enabled.
     */
    public boolean finestEnabled();
    /**
     * Writes both a message and exception to this writer.
     * The message level is "finest".
     */
    public void finest(String msg, Throwable ex);
    /**
     * Writes a message to this writer.
     * The message level is "finest".
     */
    public void finest(String msg);
    /**
     * Writes an exception to this writer.
     * The exception level is "finest".
     */
    public void finest(Throwable ex);

    /**
     * Returns a 1.4 logging handler that can be used to direct application
     * output to this GemFire logger using the standard JDK logger APIs.
     * Each time this method is called it creates a new instance of a
     * Handler so care should be taken to not call this method too often.
     */
    public Handler getHandler();
    
    /**
     * A mechanism for accessing the abstraction layer used for a plain logger.
     * @return LogWriter
     */
    public LogWriter convertToLogWriter();    
}
