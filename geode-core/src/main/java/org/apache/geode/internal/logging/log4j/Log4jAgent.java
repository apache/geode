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

import static org.apache.geode.internal.logging.Configuration.GEODE_LOGGER_PREFIX;
import static org.apache.geode.internal.logging.Configuration.MAIN_LOGGER_NAME;
import static org.apache.geode.internal.logging.Configuration.SECURITY_LOGGER_NAME;
import static org.apache.geode.internal.logging.log4j.LogWriterLevelConverter.toLevel;

import java.io.File;
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
import org.apache.logging.log4j.core.lookup.StrLookup;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import org.apache.geode.internal.logging.Configuration.LogLevelUpdateOccurs;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateScope;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogWriterLevel;
import org.apache.geode.internal.logging.ProviderAgent;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessageFactory;

public class Log4jAgent implements ProviderAgent {

  public static final String GEODE_CONSOLE_APPENDER_NAME = "STDOUT";
  public static final String LOGWRITER_APPENDER_NAME = "LOGWRITER";
  public static final String SECURITY_LOGWRITER_APPENDER_NAME = "SECURITYLOGWRITER";
  public static final String GEODE_VERBOSE_FILTER = "{GEODE_VERBOSE}";
  public static final String GEMFIRE_VERBOSE_FILTER = "{GEMFIRE_VERBOSE}";

  /**
   * Name of variable that is set to "true" in log4j2.xml to indicate that it is the default geode
   * config xml.
   */
  private static final String GEODE_DEFAULT_PROPERTY = "geode-default";

  /**
   * Finds a Log4j configuration file in the current directory. The names of the files to look for
   * are the same as those that Log4j would look for on the classpath.
   *
   * @return A File for the configuration file or null if one isn't found.
   */
  public static File findLog4jConfigInCurrentDir() {
    return ConfigLocator.findConfigInWorkingDirectory();
  }

  public static String getConfigurationInfo() {
    return getConfiguration().getConfigurationSource().toString();
  }

  private static LoggerContext getLoggerContext(final String name) {
    return ((Logger) LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE))
        .getContext();
  }

  public static boolean isUsingGemFireDefaultConfig() {
    Configuration configuration = getConfiguration();

    StrSubstitutor strSubstitutor = configuration.getStrSubstitutor();
    StrLookup variableResolver = strSubstitutor.getVariableResolver();

    String value = variableResolver.lookup(GEODE_DEFAULT_PROPERTY);

    return "true".equals(value);
  }

  public static void updateLogLevel(final Level level, final LoggerConfig... loggerConfigs) {
    for (LoggerConfig loggerConfig : loggerConfigs) {
      loggerConfig.setLevel(level);
    }
    getRootLoggerContext().updateLoggers();
  }

  public static LoggerConfig getLoggerConfig(final org.apache.logging.log4j.Logger logger) {
    return ((Logger) logger).get();
  }

  private static LoggerContext getRootLoggerContext() {
    return ((Logger) LogManager.getRootLogger()).getContext();
  }

  private static Configuration getConfiguration() {
    return getRootLoggerContext().getConfiguration();
  }

  private boolean configuredSecurityAppenders;

  public Log4jAgent() {
    // nothing
  }

  @Override
  public void configure(final LogConfig logConfig, final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope) {
    Level loggerLevel = toLevel(LogWriterLevel.find(logConfig.getLogLevel()));
    updateLogLevel(loggerLevel, getLoggerConfig(MAIN_LOGGER_NAME));

    Level securityLoggerLevel = toLevel(LogWriterLevel.find(logConfig.getSecurityLogLevel()));
    updateLogLevel(securityLoggerLevel, getLoggerConfig(SECURITY_LOGGER_NAME));

    if (!LogConfig.hasSecurityLogFile(logConfig)) {
      configuredSecurityAppenders =
          configureSecurityAppenders(SECURITY_LOGGER_NAME, securityLoggerLevel);
    }

    if (logLevelUpdateOccurs.always() ||
        logLevelUpdateOccurs.onlyWhenUsingDefaultConfig() && isUsingGemFireDefaultConfig()) {
      updateLogLevel(logConfig, logLevelUpdateScope);
    }

    configureFastLoggerDelegating();
  }

  @Override
  public void cleanup() {
    if (configuredSecurityAppenders) {
      Configuration log4jConfiguration = getRootLoggerContext().getConfiguration();
      LoggerConfig loggerConfig = log4jConfiguration.getLoggerConfig(SECURITY_LOGGER_NAME);

      loggerConfig.removeAppender(GEODE_CONSOLE_APPENDER_NAME);
      loggerConfig.removeAppender(LOGWRITER_APPENDER_NAME);

      loggerConfig.setAdditive(false);
      getRootLoggerContext().updateLoggers();
    }
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
    if (hasContextWideFilter(configuration) || hasAppenderFilter(configuration)
        || hasDebugOrLower(configuration) || hasLoggerFilter(configuration)
        || hasAppenderRefFilter(configuration)) {
      FastLogger.setDelegating(true);
    } else {
      FastLogger.setDelegating(false);
    }
  }

  private boolean hasContextWideFilter(final Configuration config) {
    return config.hasFilter();
  }

  private boolean hasAppenderFilter(final Configuration config) {
    for (Appender appender : config.getAppenders().values()) {
      if (appender instanceof AbstractFilterable) {
        if (((AbstractFilterable) appender).hasFilter()) {
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
      boolean isRoot = loggerConfig.getName().equals("");
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
      boolean isRoot = loggerConfig.getName().equals("");
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
