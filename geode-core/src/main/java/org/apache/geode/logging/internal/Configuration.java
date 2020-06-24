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
package org.apache.geode.logging.internal;

import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateScope.GEODE_LOGGERS;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigListener;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.internal.spi.LogLevelUpdateScope;
import org.apache.geode.logging.internal.spi.LogWriterLevel;
import org.apache.geode.logging.internal.spi.LoggingProvider;
import org.apache.geode.services.module.ModuleService;

/**
 * Provides logging configuration by managing a {@link LoggingProvider} for the logging backend.
 */
public class Configuration implements LogConfigListener {

  /**
   * The default {@link LogWriterLevel} is {@link LogWriterLevel#CONFIG}.
   */
  @VisibleForTesting
  public static final int DEFAULT_LOGWRITER_LEVEL = LogWriterLevel.CONFIG.intLevel();

  /**
   * Header for logging the Startup Configuration during {@code Cache} creation.
   */
  public static final String STARTUP_CONFIGURATION = "Startup Configuration: ";

  /**
   * Default logging backend configuration file used for command-line tools including GFSH.
   */
  public static final String CLI_CONFIG = "/log4j2-cli.xml";

  /**
   * System property that may be used to override which {@code LogLevelUpdateOccurs} to use.
   */
  public static final String LOG_LEVEL_UPDATE_OCCURS_PROPERTY =
      GEODE_PREFIX + "LOG_LEVEL_UPDATE_OCCURS";

  /**
   * System property that may be used to override which {@code LogLevelUpdateScope} to use.
   */
  static final String LOG_LEVEL_UPDATE_SCOPE_PROPERTY =
      GEODE_PREFIX + "LOG_LEVEL_UPDATE_SCOPE";

  private final LogLevelUpdateOccurs logLevelUpdateOccurs;
  private final LogLevelUpdateScope logLevelUpdateScope;

  private final LoggingProvider loggingProvider;

  /**
   * Protected by synchronization on Configuration instance.
   */
  private LogConfigSupplier logConfigSupplier;

  private Configuration(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final LoggingProvider loggingProvider) {
    this.logLevelUpdateOccurs = logLevelUpdateOccurs;
    this.logLevelUpdateScope = logLevelUpdateScope;
    this.loggingProvider = loggingProvider;
  }

  public static Configuration create(ModuleService moduleService) {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(),
        new LoggingProviderLoader(moduleService)
            .load());
  }

  @VisibleForTesting
  public static Configuration create(final LoggingProvider loggingProvider) {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(), loggingProvider);
  }

  @VisibleForTesting
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, ModuleService moduleService) {
    return create(logLevelUpdateOccurs, logLevelUpdateScope,
        new LoggingProviderLoader(moduleService).load());
  }

  @VisibleForTesting
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final LoggingProvider loggingProvider) {
    return new Configuration(logLevelUpdateOccurs, logLevelUpdateScope, loggingProvider);
  }

  static LogLevelUpdateOccurs getLogLevelUpdateOccurs() {
    try {
      return LogLevelUpdateOccurs.valueOf(System.getProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
          ONLY_WHEN_USING_DEFAULT_CONFIG.name()).toUpperCase());
    } catch (IllegalArgumentException e) {
      return ONLY_WHEN_USING_DEFAULT_CONFIG;
    }
  }

  static LogLevelUpdateScope getLogLevelUpdateScope() {
    try {
      return LogLevelUpdateScope.valueOf(System.getProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
          GEODE_LOGGERS.name()).toUpperCase());
    } catch (IllegalArgumentException e) {
      return GEODE_LOGGERS;
    }
  }

  /**
   * Initializes logging configuration with the {@code LogConfigSupplier}. This configuration will
   * register as a {@code LogConfigListener} and configure the {@code LoggingProvider}.
   */
  public synchronized void initialize(final LogConfigSupplier logConfigSupplier) {
    if (logConfigSupplier == null) {
      throw new IllegalArgumentException("LogConfigSupplier must not be null");
    }
    this.logConfigSupplier = logConfigSupplier;
    logConfigSupplier.addLogConfigListener(this);

    configChanged();
  }

  @Override
  public synchronized void configChanged() {
    if (logConfigSupplier == null) {
      throw new IllegalStateException("LogConfigSupplier must not be null");
    }

    LogConfig logConfig = logConfigSupplier.getLogConfig();

    loggingProvider.configure(logConfig, logLevelUpdateOccurs, logLevelUpdateScope);
  }

  /**
   * Removes this configuration as a {@code LogConfigListener} if it's currently registered and
   * cleans up the {@code LoggingProvider}.
   */
  public synchronized void shutdown() {
    if (logConfigSupplier != null) {
      logConfigSupplier.removeLogConfigListener(this);
      logConfigSupplier = null;
    }

    loggingProvider.cleanup();
  }

  String getConfigurationInfo() {
    return loggingProvider.getConfigurationInfo();
  }

  void enableLoggingToStandardOutput() {
    LogConfig logConfig = logConfigSupplier.getLogConfig();
    if (logConfig.getLogFile().exists()) {
      loggingProvider.enableLoggingToStandardOutput();
    }
  }

  void disableLoggingToStandardOutputIfLoggingToFile() {
    LogConfig logConfig = logConfigSupplier.getLogConfig();
    if (logConfig.getLogFile().exists()) {
      loggingProvider.disableLoggingToStandardOutput();
    }
  }

  @VisibleForTesting
  synchronized LogConfigSupplier getLogConfigSupplier() {
    return logConfigSupplier;
  }
}
