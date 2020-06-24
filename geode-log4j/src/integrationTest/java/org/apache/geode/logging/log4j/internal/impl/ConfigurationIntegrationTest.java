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

import static org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs.ALWAYS;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateScope.ALL_LOGGERS;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateScope.GEODE_AND_APPLICATION_LOGGERS;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateScope.GEODE_AND_SECURITY_LOGGERS;
import static org.apache.geode.logging.internal.spi.LogLevelUpdateScope.GEODE_LOGGERS;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.apache.geode.logging.internal.spi.LoggingProvider.MAIN_LOGGER_NAME;
import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;
import static org.apache.geode.logging.log4j.internal.impl.Log4jLoggingProvider.getLoggerConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.Configuration;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link Configuration}.
 */
@Category(LoggingTest.class)
public class ConfigurationIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";

  private LogConfigSupplier logConfigSupplier;
  private LogConfig logConfig;

  private Logger geodeLogger;
  private Logger geodeSecurityLogger;
  private Logger applicationLogger;

  @Before
  public void setUp() {
    logConfigSupplier = mock(LogConfigSupplier.class);
    logConfig = mock(LogConfig.class);

    when(logConfigSupplier.getLogConfig()).thenReturn(logConfig);
    when(logConfig.getLogLevel()).thenReturn(INFO.intLevel());
    when(logConfig.getSecurityLogLevel()).thenReturn(INFO.intLevel());

    geodeLogger = LogManager.getLogger(MAIN_LOGGER_NAME);
    geodeSecurityLogger = LogManager.getLogger(SECURITY_LOGGER_NAME);
    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);

    List<LoggerConfig> loggerConfigs = Arrays.asList(getLoggerConfig(geodeLogger),
        getLoggerConfig(geodeSecurityLogger), getLoggerConfig(applicationLogger));

    Log4jLoggingProvider.updateLogLevel(Level.INFO, loggerConfigs.toArray(new LoggerConfig[0]));
  }

  @Test
  public void loggerLogLevelIsInfo() {
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(geodeSecurityLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void updatesLogLevelForScopeGeodeLoggers() {
    when(logConfig.getLogLevel()).thenReturn(WARNING.intLevel());

    Configuration configuration =
        Configuration.create(ALWAYS, GEODE_LOGGERS, new ServiceLoaderModuleService(
            LogService.getLogger()));
    configuration.initialize(logConfigSupplier);

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(geodeSecurityLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void updatesLogLevelForScopeGeodeAndSecurityLoggers() {
    when(logConfig.getLogLevel()).thenReturn(WARNING.intLevel());

    Configuration configuration =
        Configuration.create(ALWAYS, GEODE_AND_SECURITY_LOGGERS, new ServiceLoaderModuleService(
            LogService.getLogger()));
    configuration.initialize(logConfigSupplier);

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(geodeSecurityLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void updatesLogLevelForScopeAllLoggers() {
    when(logConfig.getLogLevel()).thenReturn(WARNING.intLevel());

    Configuration configuration =
        Configuration.create(ALWAYS, ALL_LOGGERS, new ServiceLoaderModuleService(
            LogService.getLogger()));
    configuration.initialize(logConfigSupplier);

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(geodeSecurityLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.WARN);
  }

  @Test
  public void updatesLogLevelForScopeGeodeAndApplicationLoggers() {
    when(logConfig.getLogLevel()).thenReturn(WARNING.intLevel());

    Configuration configuration =
        Configuration.create(ALWAYS, GEODE_AND_APPLICATION_LOGGERS,
            new ServiceLoaderModuleService(LogService.getLogger()));
    configuration.initialize(logConfigSupplier);

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
    assertThat(geodeSecurityLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.WARN);
  }
}
