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
package org.apache.geode.internal.logging;

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.BreakIterator;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;

import org.apache.geode.LogWriter;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.process.StartupStatusListener;

/**
 * Abstract implementation of {@link InternalLogWriter}. Each logger has a level and it will only
 * print messages whose level is greater than or equal to the logger's level. The supported logger
 * level constants, in ascending order, are:
 * <ol>
 * <li>{@link #ALL_LEVEL}
 * <li>{@link #FINEST_LEVEL}
 * <li>{@link #FINER_LEVEL}
 * <li>{@link #FINE_LEVEL}
 * <li>{@link #CONFIG_LEVEL}
 * <li>{@link #INFO_LEVEL}
 * <li>{@link #WARNING_LEVEL}
 * <li>{@link #ERROR_LEVEL}
 * <li>{@link #SEVERE_LEVEL}
 * <li>{@link #NONE_LEVEL}
 * </ol>
 * <p>
 * Subclasses must implement:
 * <ol>
 * <li>{@link #getLogWriterLevel}
 * <li>{@link #put(int, String, Throwable)}
 * <li>{@link #put(int, StringId, Object[], Throwable)}
 * </ol>
 *
 * @deprecated Please use Log4J2 instead.
 */
@Deprecated
public abstract class LogWriterImpl implements InternalLogWriter {

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

  static {
    Assert.assertTrue(ALL_LEVEL == Level.ALL.intValue());
    Assert.assertTrue(NONE_LEVEL == Level.OFF.intValue());
    Assert.assertTrue(FINEST_LEVEL == Level.FINEST.intValue());
    Assert.assertTrue(FINER_LEVEL == Level.FINER.intValue());
    Assert.assertTrue(FINE_LEVEL == Level.FINE.intValue());
    Assert.assertTrue(CONFIG_LEVEL == Level.CONFIG.intValue());
    Assert.assertTrue(INFO_LEVEL == Level.INFO.intValue());
    Assert.assertTrue(WARNING_LEVEL == Level.WARNING.intValue());
    Assert.assertTrue(SEVERE_LEVEL == Level.SEVERE.intValue());
    int logLevels = FINEST_LEVEL | FINER_LEVEL | FINE_LEVEL | CONFIG_LEVEL | INFO_LEVEL
        | WARNING_LEVEL | SEVERE_LEVEL;
    Assert.assertTrue(logLevels == (logLevels & LOGGING_FLAGS_MASK));
    Assert.assertTrue(0 == (logLevels & SECURITY_LOGGING_FLAG));
  }

  /**
   * A listener which can be registered to be informed of startup events
   */
  private static volatile StartupStatusListener startupListener;

  private final DateFormat timeFormatter;

  protected LogWriterImpl() {
    timeFormatter = DateFormatter.createDateFormat();
  }

  /**
   * Gets the writer's level.
   */
  @Override
  public abstract int getLogWriterLevel();

  @Override
  public boolean isSecure() {
    return false;
  }

  public static String allowedLogLevels() {
    StringBuilder sb = new StringBuilder(64);
    for (int i = 0; i < levelNames.length; i++) {
      if (i != 0) {
        sb.append('|');
      }
      sb.append(levelNames[i]);
    }
    return sb.toString();
  }

  /**
   * Gets the string representation for the given <code>level</code> int code.
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
   * Handles the special cases for {@link #levelToString(int)} including such cases as the
   * SECURITY_LOGGING_PREFIX and an invalid log level.
   */
  private static String levelToStringSpecialCase(int levelWithFlags) {
    if ((levelWithFlags & SECURITY_LOGGING_FLAG) != 0) {
      // We know the flag is set so XOR will zero it out.
      int level = levelWithFlags ^ SECURITY_LOGGING_FLAG;
      return SecurityLogWriter.SECURITY_PREFIX + levelToString(level);
    } else {
      // Needed to prevent infinite recursion
      // This signifies an unknown log level was used
      return "level-" + levelWithFlags;
    }
  }

  protected static int getRealLogLevel(int levelWithFlags) {
    if (levelWithFlags == NONE_LEVEL) {
      return levelWithFlags;
    }
    return levelWithFlags & LOGGING_FLAGS_MASK;
  }

  public static String join(Object[] array) {
    return join(array, " ");
  }

  public static String join(Object[] array, String joinString) {
    return join(Arrays.asList(array), joinString);
  }

  public static String join(List<?> list) {
    return join(list, " ");
  }

  public static String join(List<?> list, String joinString) {
    StringBuilder result = new StringBuilder(80);
    boolean firstTime = true;
    Iterator it = list.iterator();
    while (it.hasNext()) {
      if (firstTime) {
        firstTime = false;
      } else {
        result.append(joinString);
      }
      result.append(it.next());
    }
    return result.toString();
  }

  /**
   * Gets the level code for the given <code>levelName</code>.
   *
   * @throws IllegalArgumentException if an unknown level name is given.
   */
  public static int levelNameToCode(String levelName) {
    if ("all".equalsIgnoreCase(levelName)) {
      return ALL_LEVEL;
    }
    if ("finest".equalsIgnoreCase(levelName) || "trace".equalsIgnoreCase(levelName)) {
      return FINEST_LEVEL;
    }
    if ("finer".equalsIgnoreCase(levelName)) {
      return FINER_LEVEL;
    }
    if ("fine".equalsIgnoreCase(levelName) || "debug".equalsIgnoreCase(levelName)) {
      return FINE_LEVEL;
    }
    if ("config".equalsIgnoreCase(levelName)) {
      return CONFIG_LEVEL;
    }
    if ("info".equalsIgnoreCase(levelName)) {
      return INFO_LEVEL;
    }
    if ("warning".equalsIgnoreCase(levelName) || "warn".equalsIgnoreCase(levelName)) {
      return WARNING_LEVEL;
    }
    if ("error".equalsIgnoreCase(levelName)) {
      return ERROR_LEVEL;
    }
    if ("severe".equalsIgnoreCase(levelName) || "fatal".equalsIgnoreCase(levelName)) {
      return SEVERE_LEVEL;
    }
    if ("none".equalsIgnoreCase(levelName)) {
      return NONE_LEVEL;
    }
    try {
      if (levelName.startsWith("level-")) {
        String levelValue = levelName.substring("level-".length());
        return Integer.parseInt(levelValue);
      }
    } catch (NullPointerException | NumberFormatException ignore) {
      // ignored
    }
    throw new IllegalArgumentException(
        "Unknown log-level \"" + levelName + "\". Valid levels are: " + join(levelNames) + ".");
  }

  /**
   * Gets a String representation of the current time.
   *
   * @return a String representation of the current time.
   */
  protected String getTimeStamp() {
    return formatDate(new Date());
  }

  /**
   * Convert a Date to a timestamp String.
   *
   * @param date a Date to format as a timestamp String.
   * @return a String representation of the current time.
   */
  protected String formatDate(Date date) {
    try {
      synchronized (timeFormatter) {
        // Need sync: see bug 21858
        return timeFormatter.format(date);
      }
    } catch (Exception e1) {
      // Fix bug 21857
      try {
        return date.toString();
      } catch (Exception e2) {
        try {
          return Long.toString(date.getTime());
        } catch (Exception e3) {
          return "timestampFormatFailed";
        }
      }
    }
  }

  /**
   * Returns true if "severe" log messages are enabled. Returns false if "severe" log messages are
   * disabled.
   */
  @Override
  public boolean severeEnabled() {
    return getLogWriterLevel() <= SEVERE_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   */
  @Override
  public void severe(String message, Throwable throwable) {
    if (severeEnabled()) {
      put(SEVERE_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "severe".
   */
  @Override
  public void severe(String message) {
    severe(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "severe".
   */
  @Override
  public void severe(Throwable throwable) {
    this.severe("", throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId, Object[] parameters, Throwable throwable) {
    if (severeEnabled()) {
      put(SEVERE_LEVEL, messageId, parameters, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId, Object parameter, Throwable throwable) {
    if (severeEnabled()) {
      put(SEVERE_LEVEL, messageId, new Object[] {parameter}, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId, Throwable throwable) {
    severe(messageId, null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId, Object[] parameters) {
    severe(messageId, parameters, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId, Object parameter) {
    severe(messageId, parameter, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "severe".
   *
   * @since GemFire 6.0
   */
  @Override
  public void severe(StringId messageId) {
    severe(messageId, null, null);
  }

  /**
   * @return true if "error" log messages are enabled.
   */
  @Override
  public boolean errorEnabled() {
    return getLogWriterLevel() <= ERROR_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   */
  @Override
  public void error(String message, Throwable throwable) {
    if (errorEnabled()) {
      put(ERROR_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "error".
   */
  @Override
  public void error(String message) {
    error(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "error".
   */
  @Override
  public void error(Throwable throwable) {
    this.error("", throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId, Object[] parameters, Throwable throwable) {
    if (errorEnabled()) {
      put(ERROR_LEVEL, messageId, parameters, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId, Object parameter, Throwable throwable) {
    if (errorEnabled()) {
      put(ERROR_LEVEL, messageId, new Object[] {parameter}, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId, Throwable throwable) {
    error(messageId, null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId, Object[] parameters) {
    error(messageId, parameters, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId, Object parameter) {
    error(messageId, parameter, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "error".
   *
   * @since GemFire 6.0
   */
  @Override
  public void error(StringId messageId) {
    error(messageId, null, null);
  }

  /**
   * @return true if "warning" log messages are enabled.
   */
  @Override
  public boolean warningEnabled() {
    return getLogWriterLevel() <= WARNING_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   */
  @Override
  public void warning(String message, Throwable throwable) {
    if (warningEnabled()) {
      put(WARNING_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "warning".
   */
  @Override
  public void warning(String message) {
    warning(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "warning".
   */
  @Override
  public void warning(Throwable throwable) {
    this.warning("", throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId, Object[] parameters, Throwable throwable) {
    if (warningEnabled()) {
      put(WARNING_LEVEL, messageId, parameters, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId, Object parameter, Throwable throwable) {
    if (warningEnabled()) {
      put(WARNING_LEVEL, messageId, new Object[] {parameter}, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId, Throwable throwable) {
    warning(messageId, null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId, Object[] parameters) {
    warning(messageId, parameters, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId, Object parameter) {
    warning(messageId, parameter, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "warning".
   *
   * @since GemFire 6.0
   */
  @Override
  public void warning(StringId messageId) {
    warning(messageId, null, null);
  }

  /**
   * @return true if "info" log messages are enabled.
   */
  @Override
  public boolean infoEnabled() {
    return getLogWriterLevel() <= INFO_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "information".
   */
  @Override
  public void info(String message, Throwable throwable) {
    if (infoEnabled()) {
      put(INFO_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "information".
   */
  @Override
  public void info(String message) {
    info(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "information".
   */
  @Override
  public void info(Throwable throwable) {
    this.info("", throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId, Object[] parameters, Throwable throwable) {
    if (infoEnabled()) {
      put(INFO_LEVEL, messageId, parameters, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId, Object parameter, Throwable throwable) {
    if (infoEnabled()) {
      put(INFO_LEVEL, messageId, new Object[] {parameter}, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId, Throwable throwable) {
    info(messageId, null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId, Object[] parameters) {
    info(messageId, parameters, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId, Object parameter) {
    info(messageId, parameter, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "info".
   *
   * @since GemFire 6.0
   */
  @Override
  public void info(StringId messageId) {
    info(messageId, null, null);
  }

  /**
   * @return true if "config" log messages are enabled.
   */
  @Override
  public boolean configEnabled() {
    return getLogWriterLevel() <= CONFIG_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   */
  @Override
  public void config(String message, Throwable throwable) {
    if (configEnabled()) {
      put(CONFIG_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "config".
   */
  @Override
  public void config(String message) {
    config(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "config".
   */
  @Override
  public void config(Throwable throwable) {
    this.config("", throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId, Object[] parameters, Throwable throwable) {
    if (configEnabled()) {
      put(CONFIG_LEVEL, messageId, parameters, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId, Object parameter, Throwable throwable) {
    if (configEnabled()) {
      put(CONFIG_LEVEL, messageId, new Object[] {parameter}, throwable);
    }
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId, Throwable throwable) {
    config(messageId, null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId, Object[] parameters) {
    config(messageId, parameters, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId, Object parameter) {
    config(messageId, parameter, null);
  }

  /**
   * Writes both a message and exception to this writer. The message level is "config".
   *
   * @since GemFire 6.0
   */
  @Override
  public void config(StringId messageId) {
    config(messageId, null, null);
  }

  /**
   * @return true if "fine" log messages are enabled.
   */
  @Override
  public boolean fineEnabled() {
    return getLogWriterLevel() <= FINE_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "fine".
   */
  @Override
  public void fine(String message, Throwable throwable) {
    if (fineEnabled()) {
      put(FINE_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "fine".
   */
  @Override
  public void fine(String message) {
    fine(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "fine".
   */
  @Override
  public void fine(Throwable throwable) {
    fine(null, throwable);
  }

  /**
   * Returns true if "finer" log messages are enabled. Returns false if "finer" log messages are
   * disabled.
   */
  @Override
  public boolean finerEnabled() {
    return getLogWriterLevel() <= FINER_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "finer".
   */
  @Override
  public void finer(String message, Throwable throwable) {
    if (finerEnabled()) {
      put(FINER_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "finer".
   */
  @Override
  public void finer(String message) {
    finer(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "finer".
   */
  @Override
  public void finer(Throwable throwable) {
    finer(null, throwable);
  }

  /**
   * Log a method entry.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message will start with
   * <code>"ENTRY"</code> and include the class and method names.
   *
   * @param sourceClass Name of class that issued the logging request.
   * @param sourceMethod Name of the method that issued the logging request.
   */
  @Override
  public void entering(String sourceClass, String sourceMethod) {
    if (finerEnabled()) {
      finer("ENTRY " + sourceClass + ":" + sourceMethod);
    }
  }

  /**
   * Log a method return.
   * <p>
   * The logging is done using the <code>finer</code> level. The string message will start with
   * <code>"RETURN"</code> and include the class and method names.
   *
   * @param sourceClass Name of class that issued the logging request.
   * @param sourceMethod Name of the method that issued the logging request.
   */
  @Override
  public void exiting(String sourceClass, String sourceMethod) {
    if (finerEnabled()) {
      finer("RETURN " + sourceClass + ":" + sourceMethod);
    }
  }

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
  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    if (finerEnabled()) {
      finer("THROW " + sourceClass + ":" + sourceMethod, thrown);
    }
  }

  /**
   * Returns true if "finest" log messages are enabled. Returns false if "finest" log messages are
   * disabled.
   */
  @Override
  public boolean finestEnabled() {
    return getLogWriterLevel() <= FINEST_LEVEL;
  }

  /**
   * Writes both a message and exception to this writer. The message level is "finest".
   */
  @Override
  public void finest(String message, Throwable throwable) {
    if (finestEnabled()) {
      put(FINEST_LEVEL, message, throwable);
    }
  }

  /**
   * Writes a message to this writer. The message level is "finest".
   */
  @Override
  public void finest(String message) {
    finest(message, null);
  }

  /**
   * Writes an exception to this writer. The exception level is "finest".
   */
  @Override
  public void finest(Throwable throwable) {
    finest(null, throwable);
  }

  /**
   * Writes both a message and exception to this writer. If a startup listener is registered, the
   * message will be written to the listener as well to be reported to a user.
   *
   * @since GemFire 7.0
   */
  public void startup(StringId msgID, Object[] params) {
    String message = msgID.toLocalizedString(params);

    StartupStatusListener listener = startupListener;
    if (listener != null) {
      listener.setStatus(message);
    }

    if (infoEnabled()) {
      put(INFO_LEVEL, message, null);
    }
  }

  /**
   * Logs a message and an exception of the given level.
   *
   * @param messageLevel the level code for the message to log
   * @param message the actual message to log
   * @param throwable the actual Exception to log
   */
  @Override
  public abstract void put(int messageLevel, String message, Throwable throwable);

  /**
   * Logs a message and an exception of the given level.
   *
   * @param messageLevel the level code for the message to log
   * @param messageId A locale agnostic form of the message
   * @param parameters the Object arguments to plug into the message
   * @param throwable the actual Exception to log
   */
  @Override
  public abstract void put(int messageLevel, StringId messageId, Object[] parameters,
      Throwable throwable);

  /**
   * formatText manipulates \n and \r chars but supports Windows and Linux/Unix/Mac
   */
  static void formatText(PrintWriter writer, String target, int initialLength) {
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

  /**
   * Check if a message of the given level would actually be logged by this logger. This check is
   * based on the Logger effective level.
   *
   * @param level a message logging level
   * @return true if the given message level is currently being logged.
   */
  private boolean isLoggable(int level) {
    return getLogWriterLevel() <= level;
  }

  /**
   * Log a message, with associated Throwable information. If the logger is currently enabled for
   * the given message level then the given message is logged.
   *
   * @param level One of the message level identifiers, e.g. SEVERE
   * @param message The string message
   * @param thrown - Throwable associated with log message.
   */
  public void log(int level, String message, Throwable thrown) {
    if (isLoggable(level)) {
      put(level, message, thrown);
    }
  }

  /**
   * Log a message. If the logger is currently enabled for the given message level then the given
   * message is logged.
   *
   * @param level One of the message level identifiers, e.g. SEVERE
   * @param message The string message
   */
  public void log(int level, String message) {
    if (isLoggable(level)) {
      put(level, message, null);
    }
  }

  @Override
  public Handler getHandler() {
    return new GemFireHandler(this);
  }

  /** Utility to get a stack trace as a string from a Throwable */
  public static String getStackTrace(Throwable throwable) {
    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw, true));
    return sw.toString();
  }

  /**
   * Utility to periodically log a stack trace for a thread. The thread should be alive when this
   * method is invoked.
   *
   * @param toStdout whether to log to stdout or to this log writer
   * @param targetThread the thread to snapshot
   * @param interval millis to wait betweeen snaps
   * @param done when to stop snapshotting (also ceases if targetThread dies)
   */
  public void logTraces(final boolean toStdout, final Thread targetThread, final int interval,
      final AtomicBoolean done) {
    if (targetThread == null) {
      return;
    }
    Thread watcherThread =
        new LoggingThread("Stack Tracer for '" + targetThread.getName() + "'", false, () -> {
          while (!done.get()) {
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              return;
            }
            if (!done.get() && targetThread.isAlive()) {
              StringBuilder sb = new StringBuilder(500);
              if (toStdout) {
                sb.append("[trace ").append(getTimeStamp()).append("] ");
              }
              StackTraceElement[] els = targetThread.getStackTrace();
              sb.append("Stack trace for '").append(targetThread).append("'")
                  .append(LINE_SEPARATOR);
              if (els.length > 0) {
                for (int i = 0; i < els.length; i++) {
                  sb.append("\tat ").append(els[i]).append(LINE_SEPARATOR);
                }
              } else {
                sb.append("    no stack").append(LINE_SEPARATOR);
              }
              if (toStdout) {
                System.out.println(sb);
              } else {
                info(sb.toString());
              }
            }
          }
        });
    watcherThread.start();
  }

  /** Utility to get a stack trace for a thread */
  public static StringBuilder getStackTrace(Thread targetThread) {
    StringBuilder sb = new StringBuilder(500);
    StackTraceElement[] els = targetThread.getStackTrace();
    sb.append("Stack trace for '").append(targetThread).append("'").append(LINE_SEPARATOR);
    if (els.length > 0) {
      for (int i = 0; i < els.length; i++) {
        sb.append("\tat ").append(els[i]).append(LINE_SEPARATOR);
      }
    } else {
      sb.append("    no stack").append(LINE_SEPARATOR);
    }
    return sb;
  }

  @Override
  public LogWriter convertToLogWriter() {
    return this;
  }

  @Override
  public LogWriterI18n convertToLogWriterI18n() {
    return this;
  }

  public static void setStartupListener(StartupStatusListener mainListener) {
    startupListener = mainListener;
  }

  public static StartupStatusListener getStartupListener() {
    return startupListener;
  }
}
