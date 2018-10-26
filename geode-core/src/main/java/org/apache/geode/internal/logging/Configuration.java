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
import static org.apache.geode.internal.logging.Configuration.LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG;
import static org.apache.geode.internal.logging.Configuration.LogLevelUpdateScope.GEODE_LOGGERS;
import static org.apache.geode.internal.logging.InternalLogWriter.CONFIG_LEVEL;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.internal.ClassPathLoader;

/**
 * Provides logging configuration by managing an {@code AgentProvider} for the logging backend
 * provider.
 */
public class Configuration implements LogConfigListener {

  private static final Logger LOGGER = StatusLogger.getLogger();

  public static final int DEFAULT_LOGWRITER_LEVEL = CONFIG_LEVEL;

  public static final String STARTUP_CONFIGURATION = "Startup Configuration: ";

  /**
   * Default logging backend configuration file used for command-line tools including GFSH.
   */
  public static final String CLI_CONFIG = "/log4j2-cli.xml";

  public static final String GEODE_LOGGER_PREFIX = "org.apache.geode";
  public static final String MAIN_LOGGER_NAME = GEODE_LOGGER_PREFIX;
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

  /**
   * System property that may be used to override which {@code ProviderAgent} to use.
   */
  static final String PROVIDER_AGENT_NAME_PROPERTY = GEODE_PREFIX + "PROVIDER_AGENT_NAME";

  /**
   * The default {@code ProviderAgent} is {@code Log4jAgent}.
   */
  static final String DEFAULT_PROVIDER_AGENT_NAME =
      "org.apache.geode.internal.logging.log4j.Log4jAgent";

  /**
   * The default {@code LogLevelUpdateOccurs} is {@code ONLY_WHEN_USING_DEFAULT_CONFIG}.
   */
  private static final LogLevelUpdateOccurs DEFAULT_LOG_LEVEL_UPDATE_OCCURS =
      ONLY_WHEN_USING_DEFAULT_CONFIG;

  /**
   * The default {@code LogLevelUpdateScope} is {@code GEODE_LOGGERS}.
   */
  private static final LogLevelUpdateScope DEFAULT_LOG_LEVEL_UPDATE_SCOPE = GEODE_LOGGERS;

  public static Configuration create() {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(), createProviderAgent());
  }

  @TestingOnly
  public static Configuration create(final ProviderAgent providerAgent) {
    return create(getLogLevelUpdateOccurs(), getLogLevelUpdateScope(), providerAgent);
  }

  @TestingOnly
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope) {
    return create(logLevelUpdateOccurs, logLevelUpdateScope, createProviderAgent());
  }

  @TestingOnly
  public static Configuration create(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final ProviderAgent providerAgent) {
    return new Configuration(logLevelUpdateOccurs, logLevelUpdateScope, providerAgent);
  }

  static LogLevelUpdateOccurs getLogLevelUpdateOccurs() {
    try {
      return LogLevelUpdateOccurs.valueOf(System.getProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
          DEFAULT_LOG_LEVEL_UPDATE_OCCURS.name()).toUpperCase());
    } catch (IllegalArgumentException e) {
      return DEFAULT_LOG_LEVEL_UPDATE_OCCURS;
    }
  }

  static LogLevelUpdateScope getLogLevelUpdateScope() {
    try {
      return LogLevelUpdateScope.valueOf(System.getProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
          DEFAULT_LOG_LEVEL_UPDATE_SCOPE.name()).toUpperCase());
    } catch (IllegalArgumentException e) {
      return DEFAULT_LOG_LEVEL_UPDATE_SCOPE;
    }
  }

  static ProviderAgent createProviderAgent() {
    String agentClassName =
        System.getProperty(PROVIDER_AGENT_NAME_PROPERTY, DEFAULT_PROVIDER_AGENT_NAME);
    try {
      Class<? extends ProviderAgent> agentClass =
          ClassPathLoader.getLatest().forName(agentClassName).asSubclass(ProviderAgent.class);
      return agentClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException | InstantiationException
        | IllegalAccessException e) {
      LOGGER.warn("Unable to create ProviderAgent of type {}", agentClassName, e);
    }
    return new NullProviderAgent();
  }

  private final LogLevelUpdateOccurs logLevelUpdateOccurs;
  private final LogLevelUpdateScope logLevelUpdateScope;

  private final ProviderAgent providerAgent;

  /**
   * Protected by synchronization on Configuration instance.
   */
  private LogConfigSupplier logConfigSupplier;

  private Configuration(final LogLevelUpdateOccurs logLevelUpdateOccurs,
      final LogLevelUpdateScope logLevelUpdateScope, final ProviderAgent providerAgent) {
    this.logLevelUpdateOccurs = logLevelUpdateOccurs;
    this.logLevelUpdateScope = logLevelUpdateScope;
    this.providerAgent = providerAgent;
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

  @TestingOnly
  synchronized LogConfigSupplier getLogConfigSupplier() {
    return logConfigSupplier;
  }

  /**
   * Controls whether or not log level updates should be triggered.
   */
  public enum LogLevelUpdateOccurs {
    NEVER,
    ONLY_WHEN_USING_DEFAULT_CONFIG,
    ALWAYS;

    public boolean never() {
      return this == NEVER;
    }

    public boolean always() {
      return this == ALWAYS;
    }

    public boolean onlyWhenUsingDefaultConfig() {
      return this == ONLY_WHEN_USING_DEFAULT_CONFIG;
    }
  }

  /**
   * Controls the scope of which packages of loggers are updated when the log level changes.
   */
  public enum LogLevelUpdateScope {
    GEODE_LOGGERS,
    GEODE_AND_SECURITY_LOGGERS,
    GEODE_AND_APPLICATION_LOGGERS,
    ALL_LOGGERS
  }
}
