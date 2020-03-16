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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.logging.internal.Configuration.LOG_LEVEL_UPDATE_OCCURS_PROPERTY;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.net.URL;
import java.util.List;
import java.util.Properties;

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
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link DistributedSystem} and log level changes with
 * {@link GeodeConsoleAppender}.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class DistributedSystemWithLogLevelChangesIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "DistributedSystemWithLogLevelChangesIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";
  private static final String APPLICATION_LOGGER_NAME = "com.company.application";

  private static String configFilePath;

  private InternalDistributedSystem system;
  private DistributionConfig distributionConfig;
  private Logger geodeLogger;
  private Logger applicationLogger;
  private String logMessage;
  private GeodeConsoleAppender geodeConsoleAppender;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

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
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY, LogLevelUpdateOccurs.ALWAYS.name());

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    distributionConfig = system.getConfig();
    geodeLogger = LogService.getLogger();
    applicationLogger = LogService.getLogger(APPLICATION_LOGGER_NAME);
    logMessage = "Logging in " + testName.getMethodName();
    geodeConsoleAppender =
        loggerContextRule.getAppender(APPENDER_NAME, GeodeConsoleAppender.class);
  }

  @After
  public void tearDown() {
    system.disconnect();
    geodeConsoleAppender.clearLogEvents();
  }

  @Test
  public void debugNotLoggedByDefault() {
    geodeLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, getClass().getName(), Level.DEBUG);
  }

  @Test
  public void debugLoggedAfterLoweringLogLevelToFine() {
    distributionConfig.setLogLevel(FINE.intLevel());

    geodeLogger.debug(logMessage);

    assertThatLogEventsContains(logMessage, geodeLogger.getName(), Level.DEBUG);
  }

  @Test
  public void debugNotLoggedAfterRestoringLogLevelToDefault() {
    distributionConfig.setLogLevel(FINE.intLevel());

    system.getConfig().setLogLevel(CONFIG.intLevel());
    geodeLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, geodeLogger.getName(), Level.DEBUG);
  }

  @Test
  public void applicationLoggerInfoLoggedByDefault() {
    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void applicationLoggerBelowLevelUnaffectedByLoweringLogLevelChanges() {
    distributionConfig.setLogLevel(FINE.intLevel());

    applicationLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, applicationLogger.getName(), Level.DEBUG);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByLoweringLogLevelChanges() {
    distributionConfig.setLogLevel(FINE.intLevel());

    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByRaisingLogLevelChanges() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void infoStatementNotLoggedAfterRaisingLogLevelToWarning() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    geodeLogger.info(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, geodeLogger.getName(), Level.INFO);
  }

  private void assertThatLogEventsContains(String message, String loggerName, Level level) {
    List<LogEvent> logEvents = geodeConsoleAppender.getLogEvents();
    for (LogEvent logEvent : logEvents) {
      if (logEvent.getMessage().getFormattedMessage().contains(message)) {
        assertThat(logEvent.getMessage().getFormattedMessage()).isEqualTo(message);
        assertThat(logEvent.getLoggerName()).isEqualTo(loggerName);
        assertThat(logEvent.getLevel()).isEqualTo(level);
        return;
      }
    }
    fail("Expected message " + message + " not found in " + logEvents);
  }

  private void assertThatLogEventsDoesNotContain(String message, String loggerName, Level level) {
    List<LogEvent> logEvents = geodeConsoleAppender.getLogEvents();
    for (LogEvent logEvent : logEvents) {
      if (logEvent.getMessage().getFormattedMessage().contains(message) &&
          logEvent.getLoggerName().equals(loggerName) && logEvent.getLevel().equals(level)) {
        fail("Expected message " + message + " should not be contained in " + logEvents);
      }
    }
  }
}
