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
package org.apache.geode.logging;

import static org.apache.geode.alerting.spi.AlertLevel.SEVERE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.addListener;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.removeListener;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.alerting.AlertingService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Integration tests for {@link AlertingService} in a loner member.
 */
@Category(AlertingTest.class)
public class AlertingServiceWithLonerIntegrationTest {

  private InternalDistributedSystem system;
  private DistributedMember member;
  private AlertListenerMessage.Listener messageListener;
  private Logger logger;
  private String alertMessage;

  private AlertingService alertingService;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    alertMessage = "Alerting in " + testName.getMethodName();

    messageListener = spy(AlertListenerMessage.Listener.class);
    addListener(messageListener);

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

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
  public void alertMessageIsNotReceived() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }
}
