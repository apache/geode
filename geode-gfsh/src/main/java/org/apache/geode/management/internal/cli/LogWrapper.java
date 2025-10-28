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
package org.apache.geode.management.internal.cli;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.shell.GfshConfig;

/**
 * Logging wrapper for GFSH - now backed by Log4j2.
 *
 * <p>
 * NOTE: Should be used only in:
 * <ol>
 * <li>gfsh process</li>
 * <li>on a Manager "if" log is required to be sent back to gfsh too</li>
 * </ol>
 * For logging only on manager use, cache.getLogger()
 *
 * @since GemFire 7.0
 */
public class LogWrapper {
  private static final Object INSTANCE_LOCK = new Object();
  @MakeNotStatic
  private static volatile LogWrapper INSTANCE = null;

  private final Logger logger;

  private LogWrapper(Cache cache) {
    // Use Geode's LogService for consistency with rest of Geode codebase
    logger = LogService.getLogger();
  }

  /**
   * Used in the manager when logging is required to be sent back to gfsh
   */
  public static LogWrapper getInstance(Cache cache) {
    if (INSTANCE == null) {
      synchronized (INSTANCE_LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new LogWrapper(cache);
        }
      }
    }

    return INSTANCE;
  }

  /**
   * used in the gfsh process
   */
  public static LogWrapper getInstance() {
    return getInstance(null);
  }

  /**
   * Configures logging using Log4j2.
   * Log4j2 configuration is primarily handled via XML configuration files,
   * but system properties can be used for dynamic configuration.
   */
  public void configure(GfshConfig config) {
    if (config.isLoggingEnabled()) {
      // Set system properties for Log4j2 configuration
      System.setProperty("gfsh.log.file", config.getLogFilePath());
      System.setProperty("gfsh.log.level", mapJulLevelToLog4jLevel(config.getLogLevel()).name());

      // Log4j2 will automatically pick up these properties if configured in log4j2.xml
      logger.debug("GFSH logging configured: file={}, level={}",
          config.getLogFilePath(), config.getLogLevel());
    }
  }

  /**
   * Maps Java Util Logging levels to Log4j2 levels.
   */
  private Level mapJulLevelToLog4jLevel(java.util.logging.Level julLevel) {
    if (julLevel == java.util.logging.Level.SEVERE)
      return Level.ERROR;
    if (julLevel == java.util.logging.Level.WARNING)
      return Level.WARN;
    if (julLevel == java.util.logging.Level.INFO)
      return Level.INFO;
    if (julLevel == java.util.logging.Level.CONFIG)
      return Level.DEBUG;
    if (julLevel == java.util.logging.Level.FINE)
      return Level.DEBUG;
    if (julLevel == java.util.logging.Level.FINER)
      return Level.TRACE;
    if (julLevel == java.util.logging.Level.FINEST)
      return Level.TRACE;
    if (julLevel == java.util.logging.Level.OFF)
      return Level.OFF;
    return Level.INFO; // Default
  }

  /**
   * Closes the current LogWrapper.
   */
  public static void close() {
    synchronized (INSTANCE_LOCK) {
      // Log4j2 cleanup is handled by LogManager
      // No manual handler cleanup needed
      INSTANCE = null;
    }
  }

  /**
   * No-op for Log4j2 compatibility.
   * Log4j2 manages logger hierarchy differently than JUL.
   *
   * @deprecated This method is no longer needed with Log4j2
   */
  @Deprecated
  public void setParentFor(org.apache.logging.log4j.Logger otherLogger) {
    // No-op: Log4j2 handles logger hierarchy automatically
  }

  public void setLogLevel(Level newLevel) {
    // Log4j2 level changes are handled via LoggerContext
    logger.debug("Log level change requested: {}", newLevel);
  }

  public Level getLogLevel() {
    return logger.getLevel();
  }

  Logger getLogger() {
    return logger;
  }

  public boolean severeEnabled() {
    return logger.isErrorEnabled();
  }

  public void severe(String message) {
    logger.error(message);
  }

  public void severe(String message, Throwable t) {
    logger.error(message, t);
  }

  public boolean warningEnabled() {
    return logger.isWarnEnabled();
  }

  public void warning(String message) {
    logger.warn(message);
  }

  public void warning(String message, Throwable t) {
    logger.warn(message, t);
  }

  public boolean infoEnabled() {
    return logger.isInfoEnabled();
  }

  public void info(String message) {
    logger.info(message);
  }

  public void info(String message, Throwable t) {
    logger.info(message, t);
  }

  public boolean configEnabled() {
    return logger.isDebugEnabled();
  }

  public void config(String message) {
    logger.debug(message);
  }

  public void config(String message, Throwable t) {
    logger.debug(message, t);
  }

  public boolean fineEnabled() {
    return logger.isDebugEnabled();
  }

  public void fine(String message) {
    logger.debug(message);
  }

  public void fine(String message, Throwable t) {
    logger.debug(message, t);
  }

  public boolean finerEnabled() {
    return logger.isTraceEnabled();
  }

  public void finer(String message) {
    logger.trace(message);
  }

  public void finer(String message, Throwable t) {
    logger.trace(message, t);
  }

  public boolean finestEnabled() {
    return logger.isTraceEnabled();
  }

  public void finest(String message) {
    logger.trace(message);
  }

  public void finest(String message, Throwable t) {
    logger.trace(message, t);
  }
}
