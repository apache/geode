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

import static java.lang.System.lineSeparator;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;
import java.util.Properties;

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

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category(LoggingTest.class)
public class GeodeConsoleAppenderWithCacheIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "GeodeConsoleAppenderWithCacheIntegrationTest_log4j2.xml";

  private static String configFilePath;

  private InternalCache cache;
  private GeodeConsoleAppender geodeConsoleAppender;
  private File logFile;
  private Logger logger;
  private String logMessage;

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
    String name = testName.getMethodName();
    logFile = new File(temporaryFolder.getRoot(), name + ".log");

    geodeConsoleAppender =
        loggerContextRule.getAppender("STDOUT", GeodeConsoleAppender.class);
    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void logsToStdoutIfLogFileNotSpecified() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();

    logger.info(logMessage);

    LogEvent foundLogEvent = null;
    for (LogEvent logEvent : geodeConsoleAppender.getLogEvents()) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        foundLogEvent = logEvent;
        break;
      }
    }
    assertThat(foundLogEvent).as(logEventsShouldContain(logMessage, foundLogEvent)).isNotNull();
  }

  @Test
  public void stopsLoggingToStdoutWhenLoggingToLogFileStarts() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());

    cache = (InternalCache) new CacheFactory(config).create();

    logger.info(logMessage);

    LogEvent foundLogEvent = null;
    for (LogEvent logEvent : geodeConsoleAppender.getLogEvents()) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        foundLogEvent = logEvent;
        break;
      }
    }
    assertThat(foundLogEvent).as(logEventsShouldNotContain(logMessage, foundLogEvent)).isNull();
  }

  @Test
  public void resumesLoggingToStdoutWhenLoggingToLogFileStops() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());

    cache = (InternalCache) new CacheFactory(config).create();

    cache.close();

    logger.info(logMessage);

    LogEvent foundLogEvent = null;
    for (LogEvent logEvent : geodeConsoleAppender.getLogEvents()) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        foundLogEvent = logEvent;
        break;
      }
    }
    assertThat(foundLogEvent).as(logEventsShouldContain(logMessage, foundLogEvent)).isNotNull();
  }

  private String logEventsShouldContain(String logMessage, LogEvent logEvent) {
    return "Expecting:" + lineSeparator() + " " + geodeConsoleAppender.getLogEvents() +
        lineSeparator() + "to contain:" + lineSeparator() + " " + logMessage +
        lineSeparator() + "but could not find:" + lineSeparator() + " " + logEvent;
  }

  private String logEventsShouldNotContain(String logMessage, LogEvent logEvent) {
    return "Expecting:" + lineSeparator() + " " + geodeConsoleAppender.getLogEvents() +
        lineSeparator() + "to not contain:" + lineSeparator() + " " + logMessage +
        lineSeparator() + "but found:" + lineSeparator() + " " + logEvent;
  }
}
