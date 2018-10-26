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
package org.apache.geode.internal.alerting;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.addListener;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.removeListener;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
import org.apache.geode.internal.admin.remote.AlertListenerMessage.Listener;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.AlertDetails;
import org.apache.geode.test.junit.categories.AlertingTest;

@Category(AlertingTest.class)
public class DistributedSystemAlertingIntegrationTest {

  private InternalDistributedSystem system;
  private DistributedMember member;
  private AlertingService alertingService;
  private Listener alertMessageListener;
  private Logger logger;
  private String connectionName;
  private String alertMessage;
  private String exceptionMessage;
  private String threadName;
  private long threadId;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    alertMessage = "Alerting in " + testName.getMethodName();
    exceptionMessage = "Exception in " + testName.getMethodName();
    connectionName = "Member in " + testName.getMethodName();
    threadName = Thread.currentThread().getName();
    threadId = Long.valueOf(Long.toHexString(Thread.currentThread().getId()));

    Properties config = new Properties();
    config.setProperty(START_LOCATOR,
        getServerHostName() + "[" + getRandomAvailableTCPPort() + "]");
    config.setProperty(NAME, connectionName);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    member = system.getDistributedMember();
    alertingService = system.getAlertingService();

    alertMessageListener = spy(Listener.class);
    addListener(alertMessageListener);

    logger = LogService.getLogger();
  }

  @After
  public void tearDown() {
    removeListener(alertMessageListener);
    system.disconnect();
  }

  @Test
  public void alertMessageIsNotReceivedByDefault() {
    logger.warn(alertMessage);

    verifyNoMoreInteractions(alertMessageListener);
  }

  @Test
  public void alertMessageIsReceivedForLevelWarning() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    verify(alertMessageListener).receivedAlertListenerMessage(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsReceivedForLevelError() {
    alertingService.addAlertListener(member, AlertLevel.ERROR);

    logger.error(alertMessage);

    verify(alertMessageListener).receivedAlertListenerMessage(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsReceivedForLevelFatal() {
    alertingService.addAlertListener(member, AlertLevel.SEVERE);

    logger.fatal(alertMessage);

    verify(alertMessageListener).receivedAlertListenerMessage(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLevelNone() {
    alertingService.addAlertListener(member, AlertLevel.NONE);

    logger.fatal(alertMessage);

    verifyNoMoreInteractions(alertMessageListener);
  }

  @Test
  public void alertMessageIsReceivedForHigherLevels() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.error(alertMessage);
    logger.fatal(alertMessage);

    verify(alertMessageListener, times(2))
        .receivedAlertListenerMessage(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLowerLevels() {
    alertingService.addAlertListener(member, AlertLevel.SEVERE);

    logger.warn(alertMessage);
    logger.error(alertMessage);

    verifyNoMoreInteractions(alertMessageListener);
  }

  @Test
  public void alertDetailsIsCreatedByAlertMessage() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    verify(alertMessageListener).createdAlertDetails(isA(AlertDetails.class));

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());

    AlertDetails alertDetails = captor.getValue();
    assertThat(alertDetails).isNotNull().isInstanceOf(AlertDetails.class);
  }

  @Test
  public void alertDetailsAlertLevelMatches() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getAlertLevel()).isEqualTo(AlertLevel.WARNING.intLevel());
  }

  @Test
  public void alertDetailsMessageMatches() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getMsg()).isEqualTo(alertMessage);
  }

  @Test
  public void alertDetailsSenderIsNullForLocalAlert() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getSender()).isNull();
  }

  @Test
  public void alertDetailsSource() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getSource()).contains(threadName);
  }

  @Test
  public void alertDetailsConnectionName() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getConnectionName()).isEqualTo(connectionName);
  }

  @Test
  public void alertDetailsExceptionTextIsEmpty() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getExceptionText()).isEqualTo("");
  }

  @Test
  public void alertDetailsExceptionTextMatches() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage, new Exception(exceptionMessage));

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    // getExceptionText returns the full stack trace
    assertThat(alertDetails.getExceptionText()).contains(exceptionMessage);
  }

  @Test
  public void alertDetailsThreadName() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getThreadName()).isEqualTo(threadName);
  }

  @Test
  public void alertDetailsThreadId() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getTid()).isEqualTo(threadId);
  }

  @Test
  public void alertDetailsMessageTime() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    logger.warn(alertMessage);

    ArgumentCaptor<AlertDetails> captor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(alertMessageListener).createdAlertDetails(captor.capture());
    AlertDetails alertDetails = captor.getValue();

    assertThat(alertDetails.getMsgTime()).isNotNull();
  }
}
