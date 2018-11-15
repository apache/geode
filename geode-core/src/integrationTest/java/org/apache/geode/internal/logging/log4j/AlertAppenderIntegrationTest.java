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

import static org.apache.geode.internal.alerting.AlertingProviderRegistry.getNullAlertingProvider;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.Date;

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

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.alerting.AlertMessaging;
import org.apache.geode.internal.alerting.AlertingProviderRegistry;
import org.apache.geode.internal.alerting.AlertingSession;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link AlertAppender}.
 */
@Category({AlertingTest.class, LoggingTest.class})
public class AlertAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME = "AlertAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "ALERT";

  private static String configFilePath;

  private AlertAppender alertAppender;
  private InternalDistributedMember localMember;
  private AlertingSession alertingSession;
  private AlertMessaging alertMessaging;
  private AlertingProviderRegistry alertingProviderRegistry;
  private Logger logger;
  private String logMessage;

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
    alertAppender =
        loggerContextRule.getAppender(APPENDER_NAME, AlertAppender.class);

    InternalDistributedSystem internalDistributedSystem =
        mock(InternalDistributedSystem.class, RETURNS_DEEP_STUBS);
    localMember = mock(InternalDistributedMember.class);
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);

    when(internalDistributedSystem.getConfig().getName()).thenReturn(testName.getMethodName());
    when(internalDistributedSystem.getDistributedMember()).thenReturn(localMember);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(clusterDistributionManager);
    when(clusterDistributionManager.getSystem()).thenReturn(internalDistributedSystem);

    alertingSession = AlertingSession.create();
    alertMessaging = new AlertMessaging(internalDistributedSystem);
    alertingProviderRegistry = AlertingProviderRegistry.get();

    alertingSession.createSession(alertMessaging);
    alertingSession.startSession();

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    alertingSession.stopSession();
    alertingProviderRegistry.clear();
    alertAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsIsEmptyByDefault() {
    assertThat(alertAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
    LogEvent event = alertAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.WARN);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
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
  public void alertMessagingExistsAfterOnConnect() {
    alertAppender.createSession(alertMessaging);

    assertThat(alertAppender.getAlertMessaging()).isNotNull();
  }

  @Test
  public void alertMessagingDoesNotExistAfterOnDisconnect() {
    alertAppender.createSession(alertMessaging);
    alertAppender.stopSession();

    assertThat(alertAppender.getAlertMessaging()).isNull();
  }

  @Test
  public void sendsNoAlertsIfNoListeners() {
    alertAppender.createSession(alertMessaging);
    alertMessaging = spy(alertAppender.getAlertMessaging());
    alertAppender.setAlertMessaging(alertMessaging);

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
    verifyZeroInteractions(alertMessaging);
  }

  @Test
  public void sendsAlertIfListenerExists() {
    alertAppender.createSession(alertMessaging);
    alertMessaging = spy(alertAppender.getAlertMessaging());
    alertAppender.setAlertMessaging(alertMessaging);
    alertAppender.addAlertListener(localMember, AlertLevel.WARNING);

    logger.warn(logMessage);

    assertThat(alertAppender.getLogEvents()).hasSize(1);
    verify(alertMessaging).sendAlert(eq(localMember), eq(AlertLevel.WARNING), any(Date.class),
        anyString(), anyString(), isNull());
  }

  @Test
  public void isRegisteredWithAlertingProviderRegistryDuringInitialization() {
    assertThat(alertingProviderRegistry.getAlertingProvider()).isSameAs(alertAppender);
  }

  @Test
  public void isUnregisteredWithAlertingProviderRegistryDuringCleanUp() {
    alertAppender.stop();

    assertThat(alertingProviderRegistry.getAlertingProvider()).isSameAs(getNullAlertingProvider());
  }
}
