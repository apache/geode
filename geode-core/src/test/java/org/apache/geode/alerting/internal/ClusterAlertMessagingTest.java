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
package org.apache.geode.alerting.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link ClusterAlertMessaging}.
 */
@Category(AlertingTest.class)
public class ClusterAlertMessagingTest {

  private InternalDistributedSystem system;
  private InternalDistributedMember localMember;
  private DistributionConfig config;
  private DistributionManager dm;
  private AlertListenerMessageFactory alertListenerMessageFactory;
  private AlertListenerMessage alertListenerMessage;

  private ClusterAlertMessaging clusterAlertMessaging;

  @Before
  public void setUp() {
    system = mock(InternalDistributedSystem.class);
    localMember = mock(InternalDistributedMember.class);
    config = mock(DistributionConfig.class);
    dm = mock(ClusterDistributionManager.class);
    alertListenerMessageFactory = mock(AlertListenerMessageFactory.class);
    alertListenerMessage = mock(AlertListenerMessage.class);

    clusterAlertMessaging = spy(new ClusterAlertMessaging(system, dm, alertListenerMessageFactory));

    when(system.getConfig()).thenReturn(config);
    when(system.getDistributedMember()).thenReturn(localMember);
    when(config.getName()).thenReturn("name");

    when(alertListenerMessageFactory.createAlertListenerMessage(
        any(DistributedMember.class), any(AlertLevel.class), any(Date.class),
        anyString(), anyString(), anyString(), anyString())).thenReturn(alertListenerMessage);

    doNothing().when(clusterAlertMessaging)
        .processAlertListenerMessage(any(AlertListenerMessage.class));
  }

  @Test
  public void sendAlertProcessesMessageIfMemberIsLocal() {
    clusterAlertMessaging.sendAlert(localMember, AlertLevel.WARNING, new Date(), "threadName",
        "formattedMessage", "stackTrace");

    verify(clusterAlertMessaging).processAlertListenerMessage(eq(alertListenerMessage));
  }

  @Test
  public void sendAlertSendsMessageIfMemberIsRemote() {
    DistributedMember remoteMember = mock(DistributedMember.class);

    clusterAlertMessaging.sendAlert(remoteMember, AlertLevel.WARNING, new Date(), "threadName",
        "formattedMessage", "stackTrace");

    verify(dm).putOutgoing(eq(alertListenerMessage));
  }

  @Test
  public void processAlertListenerMessage_requires_ClusterDistributionManager() {
    dm = mock(DistributionManager.class);

    clusterAlertMessaging = new ClusterAlertMessaging(system, dm, alertListenerMessageFactory);

    Throwable thrown =
        catchThrowable(
            () -> clusterAlertMessaging.processAlertListenerMessage(alertListenerMessage));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }
}
