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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.AlreadyRunningException;
import org.apache.geode.management.ManagementException;

public class SystemManagementServiceTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(LENIENT);

  @Mock
  private FederatingManagerFactory federatingManagerFactory;
  @Mock
  private InternalCacheForClientAccess cache;
  @Mock
  private DistributionConfig config;
  @Mock
  private FederatingManager federatingManager;
  @Mock
  private JmxManagerAdvisor jmxManagerAdvisor;
  @Mock
  private Function<SystemManagementService, LocalManager> localManagerFactory;
  @Mock
  private ManagementAgent managementAgent;
  @Mock
  private ManagementAgentFactory managementAgentFactory;
  @Mock
  private Function<ManagementResourceRepo, NotificationHub> notificationHubFactory;
  @Mock
  private InternalDistributedSystem system;

  @Before
  public void setup() {
    when(config.getJmxManager()).thenReturn(true);

    when(system.isConnected()).thenReturn(true);
    when(system.getConfig()).thenReturn(config);
    when(system.getDistributionManager()).thenReturn(mock(DistributionManager.class));

    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.getJmxManagerAdvisor()).thenReturn(jmxManagerAdvisor);

    when(federatingManagerFactory
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(federatingManager);

    when(managementAgentFactory.create(any(), any(), any())).thenReturn(managementAgent);
    when(notificationHubFactory.apply(any())).thenReturn(mock(NotificationHub.class));
    when(localManagerFactory.apply(any())).thenReturn(mock(LocalManager.class));
  }

  @Test
  public void startManager_throws_ifIfNotWillingToBeJmxManager() {
    when(config.getJmxManager()).thenReturn(false);

    BaseManagementService service = systemManagementService();

    assertThatThrownBy(service::startManager)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifSystemIsNotConnected() {
    // Must be connected to construct the service
    when(system.isConnected()).thenReturn(true);

    BaseManagementService service = systemManagementService();

    when(system.isConnected()).thenReturn(false);

    assertThatThrownBy(service::startManager)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifServiceIsClosed() {
    BaseManagementService service = systemManagementService();

    service.close();

    assertThatThrownBy(service::startManager)
        .isInstanceOf(ManagementException.class);
  }

  @Test
  public void startManager_throws_ifExistingFederatingManagerIsAlreadyRunning() {
    BaseManagementService service = systemManagementService();

    service.startManager();

    when(federatingManager.isRunning()).thenReturn(true);

    assertThatThrownBy(service::startManager)
        .isInstanceOf(AlreadyRunningException.class);
  }

  @Test
  public void startManager_startsExistingFederatingManager_ifNotAlreadyStarted() {
    BaseManagementService service = systemManagementService();

    service.startManager();

    clearInvocations(federatingManager);
    clearInvocations(federatingManagerFactory);

    when(federatingManager.isRunning()).thenReturn(false);

    service.startManager();

    verify(federatingManager).startManager();

    // Verify that the service did not create a second federating manager
    verifyNoMoreInteractions(federatingManagerFactory);
  }

  @Test
  public void startManager_startsNewFederatingManager_ifNoExistingFederatingManager() {
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(federatingManagerFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(federatingManager).startManager();
  }

  @Test
  public void startManager_reportsManagerStarted() {
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(system).handleResourceEvent(eq(MANAGER_START), any());
  }

  @Test
  public void startManager_broadcastsJmxManagerChange() {
    BaseManagementService service = systemManagementService();

    service.startManager();

    verify(jmxManagerAdvisor, atLeastOnce()).broadcastChange();
  }

  @Test
  public void startManager_stopsFederatingManager_ifRuntimeExceptionAfterStarting() {
    BaseManagementService service = systemManagementService();

    // Called after starting federating manager
    doThrow(new RuntimeException("thrown for testing")).when(managementAgent).startAgent();

    assertThatThrownBy(service::startManager)
        .isInstanceOf(RuntimeException.class);

    verify(federatingManager).stopManager();
  }

  @Test
  public void startManager_reportsManagerStopped_ifRuntimeExceptionAfterStarting() {
    BaseManagementService service = systemManagementService();

    // Called after starting federating manager
    doThrow(new RuntimeException("thrown for testing")).when(managementAgent).startAgent();

    assertThatThrownBy(service::startManager)
        .isInstanceOf(RuntimeException.class);

    verify(system).handleResourceEvent(eq(MANAGER_STOP), any());
  }

  private BaseManagementService systemManagementService() {
    return SystemManagementService.newSystemManagementService(cache, notificationHubFactory,
        localManagerFactory, federatingManagerFactory, managementAgentFactory);
  }
}
