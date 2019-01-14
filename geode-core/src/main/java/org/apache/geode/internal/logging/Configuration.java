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

import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.logging.LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG;
import static org.apache.geode.internal.logging.LogLevelUpdateScope.GEODE_LOGGERS;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;

/**
 * Provides logging configuration by managing a {@link ProviderAgent} for the logging backend.
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
   * The root name of all Geode loggers.
   */
  public static final String GEODE_LOGGER_PREFIX = "org.apache.geode";

  /**
   * The name of the main Geode logger returned by {@link Cache#getLogger()}.
   */
  public static final String MAIN_LOGGER_NAME = GEODE_LOGGER_PREFIX;

  /**
   * The name of the security Geode logger returned by {@link Cache#getSecurityLogger()}.
   */
  public static final String SECURITY_LOGGER_NAME = GEODE_LOGGER_PREFIX + ".security";

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

  private final ProviderAgent providerAgent;

  /**
   * Protected by synchronization on Configuration instance.
   */
  private LogConfigSupplier logConfigSupplier;

  public static Configuration create() {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(), new ProviderAgentLoader()
        .findProviderAgent());
  }

  @VisibleForTesting
  public static Configuration create(final ProviderAgent providerAgent) {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(), providerAgent);
  }

  @VisibleForTesting
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope) {
    return create(logLevelUpdateOccurs, logLevelUpdateScope,
        new ProviderAgentLoader().findProviderAgent());
  }

  @VisibleForTesting
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final ProviderAgent providerAgent) {
    return new Configuration(logLevelUpdateOccurs, logLevelUpdateScope, providerAgent);
  }

  private Configuration(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final ProviderAgent providerAgent) {
    this.logLevelUpdateOccurs = logLevelUpdateOccurs;
    this.logLevelUpdateScope = logLevelUpdateScope;
    this.providerAgent = providerAgent;
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
   * register as a {@code LogConfigListener} and configure the {@code ProviderAgent}.
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

    providerAgent.configure(logConfig, logLevelUpdateOccurs, logLevelUpdateScope);
  }

  /**
   * Removes this configuration as a {@code LogConfigListener} if it's currently registered and
   * cleans up the {@code ProviderAgent}.
   */
  public synchronized void shutdown() {
    if (logConfigSupplier != null) {
      logConfigSupplier.removeLogConfigListener(this);
      logConfigSupplier = null;
    }

    providerAgent.cleanup();
  }

  String getConfigurationInfo() {
    return providerAgent.getConfigurationInfo();
  }

  void enableLoggingToStandardOutput() {
    LogConfig logConfig = logConfigSupplier.getLogConfig();
    if (logConfig.getLogFile().exists()) {
      providerAgent.enableLoggingToStandardOutput();
    }
  }

  void disableLoggingToStandardOutputIfLoggingToFile() {
    LogConfig logConfig = logConfigSupplier.getLogConfig();
    if (logConfig.getLogFile().exists()) {
      providerAgent.disableLoggingToStandardOutput();
    }
  }

  @VisibleForTesting
  synchronized LogConfigSupplier getLogConfigSupplier() {
    return logConfigSupplier;
  }
}
