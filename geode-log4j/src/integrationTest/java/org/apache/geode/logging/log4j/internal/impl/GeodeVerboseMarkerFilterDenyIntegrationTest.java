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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for using {@link LogMarker#GEMFIRE_VERBOSE} with {@code MarkerFilter} having
 * {@code onMatch} of {@code DENY} and {@code onMismatch} of {@code ACCEPT}.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class GeodeVerboseMarkerFilterDenyIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "GeodeVerboseMarkerFilterDenyIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LIST";

  private static String configFilePath;

  private ListAppender listAppender;
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
    listAppender = loggerContextRule.getListAppender(APPENDER_NAME);
    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @Test
  public void geodeVerboseShouldNotLogIfGeodeVerboseIsDeny() {
    logger.info(LogMarker.GEODE_VERBOSE, logMessage);

    List<LogEvent> events = listAppender.getEvents();

    for (LogEvent event : events) {
      assertThat(event.getMessage().getFormattedMessage()).doesNotContain(logMessage);
    }
  }

  /**
   * GEMFIRE_VERBOSE is parent of GEODE_VERBOSE so disabling GEODE_VERBOSE does not disable
   * GEMFIRE_VERBOSE.
   */
  @Test
  public void gemfireVerboseShouldLogIfGeodeVerboseIsDeny() {
    logger.info(LogMarker.GEMFIRE_VERBOSE, logMessage);

    LogEvent theLogEvent = null;
    List<LogEvent> events = listAppender.getEvents();
    for (LogEvent logEvent : events) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        theLogEvent = logEvent;
      }
    }

    assertThat(theLogEvent)
        .withFailMessage("Failed to find " + logMessage + " in " + events)
        .isNotNull();
    assertThat(theLogEvent.getMessage().getFormattedMessage()).isEqualTo(logMessage);
    assertThat(theLogEvent.getLoggerName()).isEqualTo(logger.getName());
    assertThat(theLogEvent.getLevel()).isEqualTo(Level.INFO);
  }
}
