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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

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
  private InternalDistributedMember remoteMember;
  private DistributionConfig config;
  private AlertListenerMessageFactory alertListenerMessageFactory;
  private AlertListenerMessage alertListenerMessage;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    system = mock(InternalDistributedSystem.class);
    localMember = mock(InternalDistributedMember.class);
    remoteMember = mock(InternalDistributedMember.class);
    config = mock(DistributionConfig.class);
    alertListenerMessageFactory = mock(AlertListenerMessageFactory.class);
    alertListenerMessage = mock(AlertListenerMessage.class);
  }

  @Test
  public void sendAlertProcessesMessageIfMemberIsLocal() {
    ClusterAlertMessaging clusterAlertMessaging = spyClusterAlertMessaging(
        mock(ClusterDistributionManager.class), currentThreadExecutorService());

    clusterAlertMessaging.sendAlert(localMember, AlertLevel.WARNING, Instant.now(), "threadName",
        Thread.currentThread().getId(), "formattedMessage", "stackTrace");

    verify(clusterAlertMessaging).processAlertListenerMessage(eq(alertListenerMessage));
  }

  @Test
  public void sendAlertSendsMessageIfMemberIsRemote() {
    DistributionManager dm = mock(ClusterDistributionManager.class);
    ClusterAlertMessaging clusterAlertMessaging =
        spyClusterAlertMessaging(dm, currentThreadExecutorService());

    clusterAlertMessaging.sendAlert(remoteMember, AlertLevel.WARNING, Instant.now(), "threadName",
        Thread.currentThread().getId(), "formattedMessage", "stackTrace");

    verify(dm).putOutgoing(eq(alertListenerMessage));
  }

  @Test
  public void sendAlertUsesExecutorService() {
    ExecutorService executor = currentThreadExecutorService();
    ClusterAlertMessaging clusterAlertMessaging =
        spyClusterAlertMessaging(mock(ClusterDistributionManager.class), executor);

    clusterAlertMessaging.sendAlert(remoteMember, AlertLevel.WARNING, Instant.now(), "threadName",
        Thread.currentThread().getId(), "formattedMessage", "stackTrace");

    verify(executor).submit(any(Runnable.class));
  }

  @Test
  public void processAlertListenerMessage_requires_ClusterDistributionManager() {
    ClusterAlertMessaging clusterAlertMessaging = spy(new ClusterAlertMessaging(system,
        mock(DistributionManager.class), alertListenerMessageFactory, mock(ExecutorService.class)));

    Throwable thrown = catchThrowable(
        () -> clusterAlertMessaging.processAlertListenerMessage(alertListenerMessage));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  private ClusterAlertMessaging spyClusterAlertMessaging(DistributionManager distributionManager,
      ExecutorService executorService) {
    when(alertListenerMessageFactory.createAlertListenerMessage(any(DistributedMember.class),
        any(AlertLevel.class), any(Instant.class), anyString(), anyString(), anyLong(), anyString(),
        anyString()))
            .thenReturn(alertListenerMessage);
    when(config.getName())
        .thenReturn("name");
    when(system.getConfig())
        .thenReturn(config);
    when(system.getDistributedMember())
        .thenReturn(localMember);
    return spy(new ClusterAlertMessaging(system, distributionManager, alertListenerMessageFactory,
        executorService));
  }

  private ExecutorService currentThreadExecutorService() {
    ExecutorService executor = mock(ExecutorService.class);
    when(executor.submit(isA(Runnable.class)))
        .thenAnswer((Answer<Future<?>>) invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return CompletableFuture.completedFuture(null);
        });
    return executor;
  }
}
