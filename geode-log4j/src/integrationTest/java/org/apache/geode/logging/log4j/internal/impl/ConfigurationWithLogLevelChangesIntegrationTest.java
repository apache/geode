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

import static org.apache.geode.logging.internal.Configuration.create;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.logging.internal.Configuration;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.internal.spi.LogLevelUpdateScope;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link Configuration} and log level changes with
 * {@link GeodeConsoleAppender}.
 */
@Category(LoggingTest.class)
public class ConfigurationWithLogLevelChangesIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "ConfigurationWithLogLevelChangesIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";
  private static final String APPLICATION_LOGGER_NAME = "com.company.application";

  private static String configFilePath;

  private LogConfig config;
  private Configuration configuration;
  private Logger geodeLogger;
  private Logger applicationLogger;
  private String logMessage;
  private GeodeConsoleAppender geodeConsoleAppender;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() {
    config = mock(LogConfig.class);
    when(config.getLogLevel()).thenReturn(CONFIG.intLevel());
    when(config.getSecurityLogLevel()).thenReturn(CONFIG.intLevel());

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    configuration = create(LogLevelUpdateOccurs.ALWAYS, LogLevelUpdateScope.GEODE_LOGGERS,
        new ServiceLoaderModuleService(LogService.getLogger()));
    configuration.initialize(logConfigSupplier);

    geodeLogger = LogService.getLogger();
    applicationLogger = LogService.getLogger(APPLICATION_LOGGER_NAME);

    logMessage = "Logging in " + testName.getMethodName();

    geodeConsoleAppender =
        loggerContextRule.getAppender(APPENDER_NAME, GeodeConsoleAppender.class);
  }

  @After
  public void tearDown() {
    geodeConsoleAppender.clearLogEvents();
    configuration.shutdown();
  }

  @Test
  public void geodeLoggerDebugNotLoggedByDefault() {
    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void geodeLoggerDebugLoggedAfterLoweringLogLevelToFine() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE.intLevel());
    configuration.configChanged();

    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
    LogEvent event = geodeConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.DEBUG);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void geodeLoggerDebugNotLoggedAfterRestoringLogLevelToDefault() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE.intLevel());
    configuration.configChanged();

    // re-arrange
    geodeConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(CONFIG.intLevel());
    configuration.configChanged();

    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void applicationLoggerBelowLevelUnaffectedByLoweringLogLevelChanges() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE.intLevel());
    configuration.configChanged();

    // act
    applicationLogger.debug(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void applicationLoggerInfoLoggedByDefault() {
    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
    LogEvent event = geodeConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(APPLICATION_LOGGER_NAME);
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByLoweringLogLevelChanges() {
    // arrange
    geodeConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(FINE.intLevel());
    configuration.configChanged();

    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByRaisingLogLevelChanges() {
    // arrange
    geodeConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(WARNING.intLevel());
    configuration.configChanged();

    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void infoStatementNotLoggedAfterRaisingLogLevelToWarning() {
    // arrange
    geodeConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(WARNING.intLevel());
    configuration.configChanged();

    // act
    geodeLogger.info(logMessage);

    // assert
    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }
}
