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
package org.apache.geode.alerting.internal.api;

import static org.apache.geode.alerting.internal.spi.AlertLevel.ERROR;
import static org.apache.geode.alerting.internal.spi.AlertLevel.NONE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.SEVERE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.WARNING;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.addListener;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.removeListener;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.AlertDetails;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Integration tests for {@link AlertingService} in a cluster member.
 */
@Category(AlertingTest.class)
public class AlertingServiceWithClusterIntegrationTest {

  private static final long TIMEOUT = getTimeout().toMillis();

  private InternalDistributedSystem system;
  private DistributedMember member;
  private AlertListenerMessage.Listener messageListener;
  private Logger logger;
  private String connectionName;
  private String alertMessage;
  private String exceptionMessage;
  private String threadName;
  private long threadId;

  private AlertingService alertingService;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    alertMessage = "Alerting in " + testName.getMethodName();
    exceptionMessage = "Exception in " + testName.getMethodName();
    connectionName = "Member in " + testName.getMethodName();
    threadName = Thread.currentThread().getName();
    threadId = Thread.currentThread().getId();

    messageListener = spy(AlertListenerMessage.Listener.class);
    addListener(messageListener);

    String startLocator = getServerHostName() + "[" + getRandomAvailableTCPPort() + "]";

    Properties config = new Properties();
    config.setProperty(START_LOCATOR, startLocator);
    config.setProperty(NAME, connectionName);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);
    member = system.getDistributedMember();
    logger = LogService.getLogger();

    alertingService = system.getAlertingService();
  }

  @After
  public void tearDown() {
    removeListener(messageListener);
    system.disconnect();
  }

  @Test
  public void alertMessageIsNotReceivedWithoutListener() {
    logger.fatal(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertMessageIsReceivedForListenerLevelWarning() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageProcessingDoesNotTriggerAdditionalAlertMessage() {
    alertingService.addAlertListener(member, WARNING);
    logger = spy(logger);

    String recursiveAlert = "Recursive Alert";
    doAnswer(invocation -> {
      logger.warn(recursiveAlert);
      return null;
    }).when(messageListener).received(isA(AlertListenerMessage.class));

    logger.warn(alertMessage);

    verify(messageListener, timeout(TIMEOUT).times(1)).received(isA(AlertListenerMessage.class));
    verify(logger, timeout(TIMEOUT).times(1)).warn(eq(recursiveAlert));
  }

  @Test
  public void alertMessageIsReceivedForListenerLevelError() {
    alertingService.addAlertListener(member, ERROR);

    logger.error(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsReceivedForListenerLevelFatal() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLevelNone() {
    alertingService.addAlertListener(member, NONE);

    logger.fatal(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertMessageIsReceivedForHigherLevels() {
    alertingService.addAlertListener(member, WARNING);

    logger.error(alertMessage);
    logger.fatal(alertMessage);

    verify(messageListener, timeout(TIMEOUT).times(2)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLowerLevels() {
    alertingService.addAlertListener(member, SEVERE);

    logger.warn(alertMessage);
    logger.error(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertDetailsIsCreatedByAlertMessage() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails()).isNotNull().isInstanceOf(AlertDetails.class);
  }

  @Test
  public void alertDetailsAlertLevelMatches() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getAlertLevel()).isEqualTo(SEVERE.intLevel());
  }

  @Test
  public void alertDetailsMessageMatches() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getMsg()).isEqualTo(alertMessage);
  }

  @Test
  public void alertDetailsSenderIsNullForLocalAlert() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getSender()).isNull();
  }

  @Test
  public void alertDetailsSource() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getSource()).contains(threadName);
  }

  @Test
  public void alertDetailsConnectionName() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getConnectionName()).isEqualTo(connectionName);
  }

  @Test
  public void alertDetailsExceptionTextIsEmpty() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getExceptionText()).isEqualTo("");
  }

  @Test
  public void alertDetailsExceptionTextMatches() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage, new Exception(exceptionMessage));

    assertThat(captureAlertDetails().getExceptionText()).contains(exceptionMessage);
  }

  @Test
  public void alertDetailsThreadName() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getThreadName()).isEqualTo(threadName);
  }

  @Test
  public void alertDetailsThreadId() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getTid()).isEqualTo(threadId);
  }

  @Test
  public void alertDetailsMessageTime() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    assertThat(captureAlertDetails().getMsgTime()).isNotNull();
  }

  private AlertDetails captureAlertDetails() {
    ArgumentCaptor<AlertDetails> alertDetailsCaptor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(messageListener, timeout(TIMEOUT)).created(alertDetailsCaptor.capture());
    return alertDetailsCaptor.getValue();
  }
}
