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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.lookup.StrLookup;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.internal.logging.log4j.AppenderContext;
import org.apache.geode.internal.logging.log4j.ConfigLocator;
import org.apache.geode.internal.logging.log4j.Configurator;
import org.apache.geode.internal.logging.log4j.FastLogger;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessageFactory;

/**
 * Centralizes log configuration and initialization.
 */
public class LogService extends LogManager {

  private static final Logger LOGGER = StatusLogger.getLogger();

  // This is highest point in the hierarchy for all Geode logging
  public static final String ROOT_LOGGER_NAME = "";
  public static final String BASE_LOGGER_NAME = "org.apache.geode";
  public static final String MAIN_LOGGER_NAME = "org.apache.geode";
  public static final String SECURITY_LOGGER_NAME = "org.apache.geode.security";

  public static final String GEODE_VERBOSE_FILTER = "{GEODE_VERBOSE}";
  public static final String GEMFIRE_VERBOSE_FILTER = "{GEMFIRE_VERBOSE}";
  public static final String DEFAULT_CONFIG = "/log4j2.xml";
  public static final String CLI_CONFIG = "/log4j2-cli.xml";

  protected static final String STDOUT = "STDOUT";

  private static final PropertyChangeListener propertyChangeListener =
      new PropertyChangeListenerImpl();

  /**
   * Name of variable that is set to "true" in log4j2.xml to indicate that it is the default geode
   * config xml.
   */
  private static final String GEMFIRE_DEFAULT_PROPERTY = "geode-default";

  /**
   * Protected by static synchronization. Used for removal and adding stdout back in.
   */
  private static Appender stdoutAppender;

  static {
    init();
  }

  private LogService() {
    // do not instantiate
  }

  private static void init() {
    LoggerContext loggerContext = getLoggerContext(BASE_LOGGER_NAME);
    loggerContext.removePropertyChangeListener(propertyChangeListener);
    loggerContext.addPropertyChangeListener(propertyChangeListener);
    loggerContext.reconfigure(); // propertyChangeListener invokes configureFastLoggerDelegating
    configureLoggers(false, false);
  }

  public static void reconfigure() {
    init();
  }

  public static void configureLoggers(final boolean hasLogFile, final boolean hasSecurityLogFile) {
    Configurator.getOrCreateLoggerConfig(BASE_LOGGER_NAME, true, false);
    Configurator.getOrCreateLoggerConfig(MAIN_LOGGER_NAME, true, hasLogFile);
    boolean useMainLoggerForSecurity = !hasSecurityLogFile;
    Configurator.getOrCreateLoggerConfig(SECURITY_LOGGER_NAME, useMainLoggerForSecurity,
        hasSecurityLogFile);
  }

  public static AppenderContext getAppenderContext() {
    return new AppenderContext();
  }

  public static AppenderContext getAppenderContext(final String name) {
    return new AppenderContext(name);
  }

  public static boolean isUsingGemFireDefaultConfig() {
    Configuration configuration = getConfiguration();

    StrSubstitutor strSubstitutor = configuration.getStrSubstitutor();
    StrLookup variableResolver = strSubstitutor.getVariableResolver();

    String value = variableResolver.lookup(GEMFIRE_DEFAULT_PROPERTY);

    return "true".equals(value);
  }

  public static String getConfigurationInfo() {
    return getConfiguration().getConfigurationSource().toString();
  }

  /**
   * Finds a Log4j configuration file in the current directory. The names of the files to look for
   * are the same as those that Log4j would look for on the classpath.
   *
   * @return A File for the configuration file or null if one isn't found.
   */
  public static File findLog4jConfigInCurrentDir() {
    return ConfigLocator.findConfigInWorkingDirectory();
  }

  /**
   * Returns a Logger with the name of the calling class.
   *
   * @return The Logger for the calling class.
   */
  public static Logger getLogger() {
    return new FastLogger(
        LogManager.getLogger(getClassName(2), GemFireParameterizedMessageFactory.INSTANCE));
  }

  public static Logger getLogger(final String name) {
    return new FastLogger(LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE));
  }

  /**
   * Returns a LogWriterLogger that is decorated with the LogWriter and LogWriterI18n methods.
   * <p>
   * This is the bridge to LogWriter and LogWriterI18n that we need to eventually stop using in
   * phase 1. We will switch over from a shared LogWriterLogger instance to having every GemFire
   * class own its own private static GemFireLogger
   *
   * @return The LogWriterLogger for the calling class.
   */
  public static LogWriterLogger createLogWriterLogger(final String name,
      final String connectionName, final boolean isSecure) {
    return LogWriterLogger.create(name, connectionName, isSecure);
  }

  /**
   * Return the Log4j Level associated with the int level.
   *
   * @param intLevel The int value of the Level to return.
   *
   * @return The Level.
   *
   * @throws IllegalArgumentException if the Level int is not registered.
   */
  public static Level toLevel(final int intLevel) {
    for (Level level : Level.values()) {
      if (level.intLevel() == intLevel) {
        return level;
      }
    }

    throw new IllegalArgumentException("Unknown int level [" + intLevel + "].");
  }

  /**
   * Gets the class name of the caller in the current stack at the given {@code depth}.
   *
   * @param depth a 0-based index in the current stack.
   *
   * @return a class name
   */
  public static String getClassName(final int depth) {
    return new Throwable().getStackTrace()[depth].getClassName();
  }

  public static Configuration getConfiguration() {
    return getRootLoggerContext().getConfiguration();
  }

  public static void configureFastLoggerDelegating() {
    Configuration configuration = getConfiguration();
    if (Configurator.hasContextWideFilter(configuration)
        || Configurator.hasAppenderFilter(configuration)
        || Configurator.hasDebugOrLower(configuration)
        || Configurator.hasLoggerFilter(configuration)
        || Configurator.hasAppenderRefFilter(configuration)) {
      FastLogger.setDelegating(true);
    } else {
      FastLogger.setDelegating(false);
    }
  }

  public static void setSecurityLogLevel(Level level) {
    Configurator.setLevel(SECURITY_LOGGER_NAME, level);
  }

  public static Level getBaseLogLevel() {
    return Configurator.getLevel(BASE_LOGGER_NAME);
  }

  public static void setBaseLogLevel(Level level) {
    if (isUsingGemFireDefaultConfig()) {
      Configurator.setLevel(ROOT_LOGGER_NAME, level);
    }
    Configurator.setLevel(BASE_LOGGER_NAME, level);
    Configurator.setLevel(MAIN_LOGGER_NAME, level);
  }

  private static LoggerContext getLoggerContext(final String name) {
    return ((org.apache.logging.log4j.core.Logger) LogManager
        .getLogger(name, GemFireParameterizedMessageFactory.INSTANCE)).getContext();
  }

  static LoggerContext getRootLoggerContext() {
    return ((org.apache.logging.log4j.core.Logger) LogManager.getRootLogger()).getContext();
  }

  /**
   * Removes STDOUT ConsoleAppender from ROOT logger. Only called when using the log4j2-default.xml
   * configuration. This is done when creating the LogWriterAppender for log-file. The Appender
   * instance is stored in stdoutAppender so it can be restored later using restoreConsoleAppender.
   */
  public static synchronized void removeConsoleAppender() {
    AppenderContext appenderContext = getAppenderContext(ROOT_LOGGER_NAME);
    LoggerConfig loggerConfig = appenderContext.getLoggerConfig();
    Appender stdoutAppender = loggerConfig.getAppenders().get(STDOUT);
    if (stdoutAppender != null) {
      loggerConfig.removeAppender(STDOUT);
      LogService.stdoutAppender = stdoutAppender;
      appenderContext.getLoggerContext().updateLoggers();
    }
  }

  /**
   * Restores STDOUT ConsoleAppender to ROOT logger. Only called when using the log4j2-default.xml
   * configuration. This is done when the LogWriterAppender for log-file is destroyed. The Appender
   * instance stored in stdoutAppender is used.
   */
  public static synchronized void restoreConsoleAppender() {
    if (stdoutAppender == null) {
      return;
    }
    AppenderContext appenderContext = getAppenderContext(ROOT_LOGGER_NAME);
    LoggerConfig loggerConfig = appenderContext.getLoggerConfig();
    Appender stdoutAppender = loggerConfig.getAppenders().get(STDOUT);
    if (stdoutAppender == null) {
      loggerConfig.addAppender(LogService.stdoutAppender, Level.ALL, null);
      appenderContext.getLoggerContext().updateLoggers();
    }
  }

  private static class PropertyChangeListenerImpl implements PropertyChangeListener {

    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
      LOGGER.debug("LogService responding to a property change event. Property name is {}.",
          evt.getPropertyName());

      if (evt.getPropertyName().equals(LoggerContext.PROPERTY_CONFIG)) {
        configureFastLoggerDelegating();
      }
    }
  }
}
