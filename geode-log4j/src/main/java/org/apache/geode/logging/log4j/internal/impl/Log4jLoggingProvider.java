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
package org.apache.geode.logging.log4j.internal.impl;

import static org.apache.geode.logging.internal.log4j.LogWriterLevelConverter.toLevel;

import java.util.Collection;
import java.util.HashSet;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.AbstractFilterable;
import org.apache.logging.log4j.core.filter.Filterable;
import org.apache.logging.log4j.core.lookup.StrLookup;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.internal.spi.LogLevelUpdateScope;
import org.apache.geode.logging.internal.spi.LogWriterLevel;
import org.apache.geode.logging.internal.spi.LoggingProvider;

/**
 * Log4J 2 implementation of {@link LoggingProvider}.
 *
 * <p>
 * Returned Logger is wrapped inside an instance of {@link FastLogger} which skips expensive
 * filtering, debug and trace handling with a volatile boolean. This optimization is turned on only
 * when using the default Geode {@code log4j2.xml} by checking for the existence of this property:
 *
 * <pre>
 * &lt;Property name="geode-default"&gt;true&lt;/Property&gt;
 * </pre>
 *
 * <p>
 * Returned Logger uses {@code GemFireParameterizedMessageFactory} to create
 * {@code GemFireParameterizedMessage} which excludes {@code Region}s from being handled as a
 * {@code Map} and {@code EntriesCollection} from being handled as a {@code Collection}. Without
 * this change, using a {@code Region} or {@code EntriesCollection} in a log statement can result in
 * an expensive operation or even a hang in the case of a {@code PartitionedRegion}.
 */
public class Log4jLoggingProvider implements LoggingProvider {

  static final String GEODE_CONSOLE_APPENDER_NAME = "STDOUT";
  static final String LOGWRITER_APPENDER_NAME = "LOGWRITER";
  static final String SECURITY_LOGWRITER_APPENDER_NAME = "SECURITYLOGWRITER";

  private static final String GEODE_VERBOSE_FILTER = "{GEODE_VERBOSE}";
  private static final String GEMFIRE_VERBOSE_FILTER = "{GEMFIRE_VERBOSE}";

  /**
   * Name of variable that is set to "true" in log4j2.xml to indicate that it is the default geode
   * config xml.
   */
  private static final String GEODE_DEFAULT_PROPERTY = "geode-default";

  public static void updateLogLevel(final Level level, final LoggerConfig... loggerConfigs) {
    for (LoggerConfig loggerConfig : loggerConfigs) {
      loggerConfig.setLevel(level);
    }
    getRootLoggerContext().updateLoggers();
  }

  public static LoggerConfig getLoggerConfig(final org.apache.logging.log4j.Logger logger) {
    return ((Logger) logger).get();
  }

  @VisibleForTesting
  static String getConfigurationInfoString() {
    return getConfiguration().getConfigurationSource().toString();
  }

  static boolean isUsingGemFireDefaultConfig() {
    Configuration configuration = getConfiguration();

    StrSubstitutor strSubstitutor = configuration.getStrSubstitutor();
    StrLookup variableResolver = strSubstitutor.getVariableResolver();

    String value = variableResolver.lookup(GEODE_DEFAULT_PROPERTY);

    return "true".equals(value);
  }

  /**
   * Retrieves the Log4j Core LoggerContext, with defensive checks to prevent SLF4J binding
   * conflicts.
   *
   * <p>
   * JAKARTA MIGRATION NOTE: This method was enhanced during the Jakarta EE / Spring Boot 3.x
   * migration
   * to handle potential SLF4J binding conflicts. Spring Boot 3.x includes 'log4j-to-slf4j' by
   * default,
   * which can cause ClassCastException when LogManager.getRootLogger() returns an SLF4J logger
   * instead
   * of a Log4j logger.
   *
   * <p>
   * The defensive instanceof checks ensure we safely handle cases where:
   * <ul>
   * <li>LogManager.getRootLogger() returns org.apache.logging.slf4j.SLF4JLogger (SLF4J binding
   * active)</li>
   * <li>LogManager.getContext() returns org.apache.logging.slf4j.SLF4JLoggerContext (SLF4J binding
   * active)</li>
   * <li>Normal case: returns org.apache.logging.log4j.core.Logger and LoggerContext</li>
   * </ul>
   *
   * <p>
   * Global Fix: The root build.gradle excludes 'log4j-to-slf4j' to prevent the circular binding,
   * but this defensive code provides additional safety.
   *
   * @return the Log4j Core LoggerContext
   * @throws IllegalStateException if Log4j LoggerContext cannot be obtained (SLF4J binding
   *         misconfiguration)
   */
  private static LoggerContext getRootLoggerContext() {
    // Explicitly use Log4j Core's ContextSelector to avoid SLF4J binding conflicts
    // This ensures we get the Log4j LoggerContext, not SLF4J's LoggerContext
    try {
      org.apache.logging.log4j.Logger rootLogger = LogManager.getRootLogger();
      if (rootLogger instanceof Logger) {
        LoggerContext context = ((Logger) rootLogger).getContext();
        if (context instanceof LoggerContext) {
          return context;
        }
      }
    } catch (ClassCastException e) {
      // Fall through to alternative approach if cast fails
    }

    // Alternative: Use Log4j's context selector directly
    org.apache.logging.log4j.spi.LoggerContext context = LogManager.getContext(false);
    if (context instanceof LoggerContext) {
      return (LoggerContext) context;
    }

    throw new IllegalStateException(
        "Unable to obtain Log4j LoggerContext. Found: " + context.getClass().getName() +
            ". This usually indicates SLF4J is binding to LogManager instead of Log4j. " +
            "Check your classpath for slf4j-reload4j, log4j-to-slf4j, or other SLF4J Log4j bindings.");
  }

  private static Configuration getConfiguration() {
    return getRootLoggerContext().getConfiguration();
  }

  private boolean configuredSecurityAppenders;

  public Log4jLoggingProvider() {
    // nothing
  }

  @Override
  public void configure(final LogConfig logConfig, final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope) {
    if (shouldUpdateLogLevels(logLevelUpdateOccurs)) {
      Level loggerLevel = toLevel(LogWriterLevel.find(logConfig.getLogLevel()));
      updateLogLevel(loggerLevel, getLoggerConfig(MAIN_LOGGER_NAME));

      Level securityLoggerLevel = toLevel(LogWriterLevel.find(logConfig.getSecurityLogLevel()));
      updateLogLevel(securityLoggerLevel, getLoggerConfig(SECURITY_LOGGER_NAME));

      if (!LogConfig.hasSecurityLogFile(logConfig)) {
        configuredSecurityAppenders =
            configureSecurityAppenders(SECURITY_LOGGER_NAME, securityLoggerLevel);
      }
    }

    if (shouldUpdateLogLevels(logLevelUpdateOccurs)) {
      updateLogLevel(logConfig, logLevelUpdateScope);
    }

    configureFastLoggerDelegating();
  }

  private boolean shouldUpdateLogLevels(final LogLevelUpdateOccurs logLevelUpdateOccurs) {
    return logLevelUpdateOccurs.always() ||
        logLevelUpdateOccurs.onlyWhenUsingDefaultConfig() && isUsingGemFireDefaultConfig();
  }

  @Override
  public void cleanup() {
    if (configuredSecurityAppenders) {
      Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
      LoggerConfig loggerConfig = log4jConfiguration.getLoggerConfig(
          SECURITY_LOGGER_NAME);

      loggerConfig.removeAppender(GEODE_CONSOLE_APPENDER_NAME);
      loggerConfig.removeAppender(LOGWRITER_APPENDER_NAME);

      loggerConfig.setAdditive(false);
      getRootLoggerContext().updateLoggers();
    }
  }

  @Override
  public String getConfigurationInfo() {
    return getConfiguration().getConfigurationSource().toString();
  }

  @Override
  public void enableLoggingToStandardOutput() {
    Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
    Appender appender = log4jConfiguration.getAppender(GEODE_CONSOLE_APPENDER_NAME);
    if (appender instanceof PausableAppender) {
      PausableAppender geodeConsoleAppender = (PausableAppender) appender;
      geodeConsoleAppender.resume();
    }
  }

  @Override
  public void disableLoggingToStandardOutput() {
    Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
    Appender appender = log4jConfiguration.getAppender(GEODE_CONSOLE_APPENDER_NAME);
    if (appender instanceof PausableAppender) {
      PausableAppender geodeConsoleAppender = (PausableAppender) appender;
      geodeConsoleAppender.pause();
    }
  }

  @Override
  public int getPriority() {
    return 0;
  }

  @Override
  public String toString() {
    return super.toString() + ": {configuredSecurityAppenders="
        + configuredSecurityAppenders + "}";
  }

  private void updateLogLevel(final LogConfig logConfig,
      final LogLevelUpdateScope logLevelUpdateScope) {
    Level level = toLevel(LogWriterLevel.find(logConfig.getLogLevel()));

    Configuration configuration =
        getRootLoggerContext().getConfiguration();

    Collection<LoggerConfig> loggerConfigs = new HashSet<>();

    for (LoggerConfig loggerConfig : configuration.getLoggers().values()) {
      switch (logLevelUpdateScope) {
        case ALL_LOGGERS:
          loggerConfigs.add(loggerConfig);
          break;
        case GEODE_AND_SECURITY_LOGGERS:
          if (loggerConfig.getName().startsWith(GEODE_LOGGER_PREFIX)) {
            loggerConfigs.add(loggerConfig);
          }
          break;
        case GEODE_AND_APPLICATION_LOGGERS:
          if (!loggerConfig.getName().equals(SECURITY_LOGGER_NAME)) {
            loggerConfigs.add(loggerConfig);
          }
          break;
        case GEODE_LOGGERS:
          if (loggerConfig.getName().startsWith(GEODE_LOGGER_PREFIX) &&
              !loggerConfig.getName().equals(SECURITY_LOGGER_NAME)) {
            loggerConfigs.add(loggerConfig);
          }
      }
    }

    updateLogLevel(level, loggerConfigs.toArray(new LoggerConfig[0]));
  }

  private boolean configureSecurityAppenders(final String name, final Level level) {
    Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
    LoggerConfig loggerConfig = log4jConfiguration.getLoggerConfig(name);

    if (!loggerConfig.getName().equals(SECURITY_LOGGER_NAME)) {
      return false;
    }

    Appender stdoutAppender = log4jConfiguration.getAppender(GEODE_CONSOLE_APPENDER_NAME);
    Appender mainLogWriterAppender = log4jConfiguration.getAppender(LOGWRITER_APPENDER_NAME);

    if (stdoutAppender != null) {
      loggerConfig.addAppender(stdoutAppender, level, null);
    }
    if (mainLogWriterAppender != null) {
      loggerConfig.addAppender(mainLogWriterAppender, level, null);
    }

    loggerConfig.setAdditive(true);

    getRootLoggerContext().updateLoggers();

    return true;
  }

  private LoggerConfig getLoggerConfig(final String name) {
    Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
    return log4jConfiguration.getLoggerConfig(name);
  }

  private void configureFastLoggerDelegating() {
    Configuration configuration = getConfiguration();
    FastLogger.setDelegating(hasContextWideFilter(configuration) || hasAppenderFilter(configuration)
        || hasDebugOrLower(configuration) || hasLoggerFilter(configuration)
        || hasAppenderRefFilter(configuration));
  }

  private boolean hasContextWideFilter(final Configuration config) {
    return config.hasFilter();
  }

  private boolean hasAppenderFilter(final Configuration config) {
    for (Appender appender : config.getAppenders().values()) {
      if (appender instanceof AbstractFilterable) {
        if (((Filterable) appender).hasFilter()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasDebugOrLower(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isDebugOrLower = loggerConfig.getLevel().isLessSpecificThan(Level.DEBUG);
      if (isDebugOrLower) {
        return true;
      }
    }
    return false;
  }

  private boolean hasLoggerFilter(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isRoot = loggerConfig.getName().isEmpty();
      boolean isGemFire = loggerConfig.getName().startsWith(GEODE_LOGGER_PREFIX);
      boolean hasFilter = loggerConfig.hasFilter();
      boolean isGemFireVerboseFilter =
          hasFilter && (GEODE_VERBOSE_FILTER.equals(loggerConfig.getFilter().toString())
              || GEMFIRE_VERBOSE_FILTER.equals(loggerConfig.getFilter().toString()));

      if (isRoot || isGemFire) {
        // check for Logger Filter
        if (hasFilter && !isGemFireVerboseFilter) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasAppenderRefFilter(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isRoot = loggerConfig.getName().isEmpty();
      boolean isGemFire = loggerConfig.getName().startsWith(GEODE_LOGGER_PREFIX);

      if (isRoot || isGemFire) {
        // check for AppenderRef Filter
        for (AppenderRef appenderRef : loggerConfig.getAppenderRefs()) {
          if (appenderRef.getFilter() != null) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
