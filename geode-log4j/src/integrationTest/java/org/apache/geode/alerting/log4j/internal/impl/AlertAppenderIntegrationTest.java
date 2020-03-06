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
package org.apache.geode.alerting.log4j.internal.impl;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.URL;
import java.time.Instant;
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
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.alerting.internal.NullAlertingService;
import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link AlertAppender}.
 */
@Category({AlertingTest.class, LoggingTest.class})
@SuppressWarnings("deprecation")
public class AlertAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME = "AlertAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "ALERT";

  private static String configFilePath;

  private InternalDistributedSystem internalDistributedSystem;
  private AlertAppender alertAppender;
  private DistributedMember localMember;
  private AlertingService alertingService;
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
    alertAppender = loggerContextRule.getAppender(APPENDER_NAME, AlertAppender.class);

    Properties configProperties = new Properties();
    configProperties.setProperty(NAME, testName.getMethodName());
    configProperties.setProperty(LOCATORS, "");

    DistributedSystem distributedSystem = DistributedSystem.connect(configProperties);
    localMember = distributedSystem.getDistributedMember();
    internalDistributedSystem = (InternalDistributedSystem) distributedSystem;
    alertingService = internalDistributedSystem.getAlertingService();

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    if (internalDistributedSystem != null) {
      internalDistributedSystem.disconnect();
    }
    alertAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsIsEmptyByDefault() {
    assertThat(alertAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    logger.warn(logMessage);

    LogEvent theLogEvent = null;
    List<LogEvent> events = alertAppender.getLogEvents();
    for (LogEvent logEvent : events) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        theLogEvent = logEvent;
      }
    }

    assertThat(theLogEvent)
        .withFailMessage("Failed to find " + logMessage + " in " + events)
        .isNotNull();
    assertThat(theLogEvent.getMessage().getFormattedMessage())
        .isEqualTo(logMessage);
    assertThat(theLogEvent.getLoggerName())
        .isEqualTo(logger.getName());
    assertThat(theLogEvent.getLevel())
        .isEqualTo(Level.WARN);
  }

  @Test
  public void skipsLogEventsWithoutMatchingAlertLevel() {
    // AlertLevelConverter does not convert trace|debug|info Level to AlertLevel
    logger.trace("trace");
    logger.debug("debug");
    logger.info("info");

    assertThat(alertAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void pausedDoesNotAppend() {
    alertAppender.pause();

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void resumeAfterPausedAppends() {
    alertAppender.pause();
    alertAppender.resume();

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void resumeWithoutPauseStillLogs() {
    alertAppender.resume();

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void isPausedReturnsTrueAfterPause() {
    alertAppender.pause();

    assertThat(alertAppender.isPaused()).isTrue();
  }

  @Test
  public void isPausedReturnsFalseAfterResume() {
    alertAppender.pause();
    alertAppender.resume();

    assertThat(alertAppender.isPaused()).isFalse();
  }

  @Test
  public void resumeWithoutPauseDoesNothing() {
    alertAppender.resume();

    assertThat(alertAppender.isPaused()).isFalse();
  }

  @Test
  public void isPausedReturnsFalseByDefault() {
    assertThat(alertAppender.isPaused()).isFalse();
  }

  @Test
  public void alertingServiceExistsAfterOnConnect() {
    alertAppender.createSession(alertingService);

    assertThat(alertAppender.getAlertingService()).isSameAs(alertingService);
  }

  @Test
  public void alertingServiceDoesNotExistAfterOnDisconnect() {
    alertAppender.createSession(alertingService);
    alertAppender.stopSession();

    assertThat(alertAppender.getAlertingService()).isSameAs(NullAlertingService.get());
  }

  @Test
  public void sendsNoAlertsIfNoListeners() {
    alertAppender.createSession(alertingService);
    alertingService = spy(alertAppender.getAlertingService());
    alertAppender.setAlertingService(alertingService);

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
    verify(alertingService).hasAlertListeners();
    verifyNoMoreInteractions(alertingService);
  }

  @Test
  public void sendsAlertIfListenerExists() {
    alertAppender.createSession(alertingService);
    alertingService = spy(alertAppender.getAlertingService());
    alertAppender.setAlertingService(alertingService);
    alertingService.addAlertListener(localMember, AlertLevel.WARNING);

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
    verify(alertingService).sendAlerts(eq(AlertLevel.WARNING), any(Instant.class), anyString(),
        anyLong(), anyString(), isNull());
  }
}
