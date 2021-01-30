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
package org.apache.geode.management.internal;

import static org.apache.geode.distributed.internal.ResourceEvent.MANAGER_START;
import static org.apache.geode.distributed.internal.ResourceEvent.MANAGER_STOP;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.AlreadyRunningException;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class SystemManagementServiceTest {

  private InternalCacheForClientAccess cache;
  private DistributionConfig config;
  private FederatingManager federatingManager;
  private FederatingManagerFactory federatingManagerFactory;
  private JmxManagerAdvisor jmxManagerAdvisor;
  private Function<SystemManagementService, LocalManager> localManagerFactory;
  private ManagementAgent managementAgent;
  private BiFunction<DistributionConfig, InternalCacheForClientAccess, ManagementAgent> managementAgentFactory;
  private Function<ManagementResourceRepo, NotificationHub> notificationHubFactory;
  private InternalDistributedSystem system;

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(InternalCacheForClientAccess.class);
    config = mock(DistributionConfig.class);
    federatingManager = mock(FederatingManager.class);
    federatingManagerFactory = mock(FederatingManagerFactory.class);
    jmxManagerAdvisor = mock(JmxManagerAdvisor.class);
    localManagerFactory = uncheckedCast(mock(Function.class));
    managementAgent = mock(ManagementAgent.class);
    managementAgentFactory = uncheckedCast(mock(BiFunction.class));
    notificationHubFactory = uncheckedCast(mock(Function.class));
    system = mock(InternalDistributedSystem.class);

    when(cache.getInternalDistributedSystem())
        .thenReturn(system);
    when(config.getJmxManager())
        .thenReturn(true);
    when(localManagerFactory.apply(any()))
        .thenReturn(mock(LocalManager.class));
    when(notificationHubFactory.apply(any()))
        .thenReturn(mock(NotificationHub.class));
    when(system.isConnected())
        .thenReturn(true);
    when(system.getConfig())
        .thenReturn(config);
    when(system.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
  }

  @Test
  public void startManager_throws_ifIfNotWillingToBeJmxManager() {
    when(config.getJmxManager())
        .thenReturn(false);
    BaseManagementService service = systemManagementService();

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifSystemIsNotConnected() {
    // Must be connected to construct the service
    when(system.isConnected())
        .thenReturn(true);
    BaseManagementService service = systemManagementService();
    when(system.isConnected())
        .thenReturn(false);

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifServiceIsClosed() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();
    service.close();

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifExistingFederatingManagerIsAlreadyRunning() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManager.isRunning())
        .thenReturn(true);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();
    service.startManager();

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(AlreadyRunningException.class);
  }

  @Test
  public void startManager_startsExistingFederatingManager_ifNotAlreadyStarted() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();
    service.startManager();
    clearInvocations(federatingManager);
    clearInvocations(federatingManagerFactory);
    when(federatingManager.isRunning())
        .thenReturn(false);

    service.startManager();

    verify(federatingManager)
        .startManager();

    // Verify that the service did not create a second federating manager
    verifyNoMoreInteractions(federatingManagerFactory);
  }

  @Test
  public void startManager_startsNewFederatingManager_ifNoExistingFederatingManager() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(federatingManagerFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(federatingManager)
        .startManager();
  }

  @Test
  public void startManager_reportsManagerStarted() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(system)
        .handleResourceEvent(eq(MANAGER_START), any());
  }

  @Test
  public void startManager_broadcastsJmxManagerChange() {
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(jmxManagerAdvisor, atLeastOnce())
        .broadcastChange();
  }

  @Test
  public void startManager_stopsFederatingManager_ifRuntimeExceptionAfterStarting() {
    doThrow(new RuntimeException("thrown for testing"))
        .when(managementAgent)
        .startAgent();
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class);
    verify(federatingManager)
        .stopManager();
  }

  @Test
  public void startManager_reportsManagerStopped_ifRuntimeExceptionAfterStarting() {
    doThrow(new RuntimeException("thrown for testing"))
        .when(managementAgent)
        .startAgent();
    when(cache.getJmxManagerAdvisor())
        .thenReturn(jmxManagerAdvisor);
    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);
    when(managementAgentFactory.apply(any(), any()))
        .thenReturn(managementAgent);
    BaseManagementService service = systemManagementService();

    Throwable thrown = catchThrowable(() -> service.startManager());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class);
    verify(system)
        .handleResourceEvent(eq(MANAGER_STOP), any());
  }

  private BaseManagementService systemManagementService() {
    return SystemManagementService.newSystemManagementService(cache, notificationHubFactory,
        localManagerFactory, federatingManagerFactory, managementAgentFactory);
  }
}
