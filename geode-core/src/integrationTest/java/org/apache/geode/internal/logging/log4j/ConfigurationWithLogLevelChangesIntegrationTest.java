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

import static org.apache.geode.internal.logging.Configuration.DEFAULT_LOGWRITER_LEVEL;
import static org.apache.geode.internal.logging.Configuration.create;
import static org.apache.geode.internal.logging.InternalLogWriter.FINE_LEVEL;
import static org.apache.geode.internal.logging.InternalLogWriter.WARNING_LEVEL;
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

import org.apache.geode.internal.logging.Configuration;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateOccurs;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateScope;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link Configuration} and log level changes with
 * {@link PausableConsoleAppender}.
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
  private PausableConsoleAppender pausableConsoleAppender;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    config = mock(LogConfig.class);
    when(config.getLogLevel()).thenReturn(DEFAULT_LOGWRITER_LEVEL);
    when(config.getSecurityLogLevel()).thenReturn(DEFAULT_LOGWRITER_LEVEL);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    configuration = create(LogLevelUpdateOccurs.ALWAYS, LogLevelUpdateScope.GEODE_LOGGERS);
    configuration.initialize(logConfigSupplier);

    geodeLogger = LogService.getLogger();
    applicationLogger = LogService.getLogger(APPLICATION_LOGGER_NAME);

    logMessage = "Logging in " + testName.getMethodName();

    pausableConsoleAppender =
        loggerContextRule.getAppender(APPENDER_NAME, PausableConsoleAppender.class);
  }

  @After
  public void tearDown() throws Exception {
    pausableConsoleAppender.clearLogEvents();
    configuration.shutdown();
  }

  @Test
  public void geodeLoggerDebugNotLoggedByDefault() {
    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void geodeLoggerDebugLoggedAfterLoweringLogLevelToFine() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE_LEVEL);
    configuration.configChanged();

    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
    LogEvent event = pausableConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.DEBUG);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void geodeLoggerDebugNotLoggedAfterRestoringLogLevelToDefault() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE_LEVEL);
    configuration.configChanged();

    // re-arrange
    pausableConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(DEFAULT_LOGWRITER_LEVEL);
    configuration.configChanged();

    // act
    geodeLogger.debug(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void applicationLoggerBelowLevelUnaffectedByLoweringLogLevelChanges() {
    // arrange
    when(config.getLogLevel()).thenReturn(FINE_LEVEL);
    configuration.configChanged();

    // act
    applicationLogger.debug(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void applicationLoggerInfoLoggedByDefault() {
    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
    LogEvent event = pausableConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(APPLICATION_LOGGER_NAME);
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByLoweringLogLevelChanges() {
    // arrange
    pausableConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(FINE_LEVEL);
    configuration.configChanged();

    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByRaisingLogLevelChanges() {
    // arrange
    pausableConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(WARNING_LEVEL);
    configuration.configChanged();

    // act
    applicationLogger.info(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void infoStatementNotLoggedAfterRaisingLogLevelToWarning() {
    // arrange
    pausableConsoleAppender.clearLogEvents();
    when(config.getLogLevel()).thenReturn(WARNING_LEVEL);
    configuration.configChanged();

    // act
    geodeLogger.info(logMessage);

    // assert
    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }
}
