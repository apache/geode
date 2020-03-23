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
 *
 */

package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.management.internal.BaseManagementService;

public class InternalLocatorTest {
  @Test
  public void startClusterManagementServiceWithRestServiceEnabledInvokesStartManager()
      throws URISyntaxException {
    LoggingSession loggingSession = mock(LoggingSession.class);
    DistributionConfigImpl distributionConfig = mock(DistributionConfigImpl.class);
    when(distributionConfig.getJmxManager()).thenReturn(true);
    when(distributionConfig.getLocators()).thenReturn("");
    when(distributionConfig.getSecurableCommunicationChannels())
        .thenReturn(new SecurableCommunicationChannel[] {});
    when(distributionConfig.getSecurityAuthTokenEnabledComponents()).thenReturn(new String[] {});
    InternalLocator internalLocator = new InternalLocator(0, loggingSession, null, null, null, null,
        null, null, distributionConfig, null);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(regionFactory);
    when(cache.getOptionalService(HttpService.class))
        .thenReturn(Optional.of(mock(HttpService.class)));
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    BaseManagementService managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    AgentUtil agentUtil = mock(AgentUtil.class);
    when(agentUtil.findWarLocation("geode-web-management")).thenReturn(new URI("management.war"));

    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(managementService).startManager();
  }

  @Test
  public void startClusterManagementServiceWithRunningManagerNeverInvokesStartManager()
      throws URISyntaxException {
    LoggingSession loggingSession = mock(LoggingSession.class);
    DistributionConfigImpl distributionConfig = mock(DistributionConfigImpl.class);
    when(distributionConfig.getJmxManager()).thenReturn(true);
    when(distributionConfig.getLocators()).thenReturn("");
    when(distributionConfig.getSecurableCommunicationChannels())
        .thenReturn(new SecurableCommunicationChannel[] {});
    when(distributionConfig.getSecurityAuthTokenEnabledComponents()).thenReturn(new String[] {});
    InternalLocator internalLocator = new InternalLocator(0, loggingSession, null, null, null, null,
        null, null, distributionConfig, null);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(regionFactory);
    when(cache.getOptionalService(HttpService.class))
        .thenReturn(Optional.of(mock(HttpService.class)));
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    BaseManagementService managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    when(managementService.isManager()).thenReturn(true);
    AgentUtil agentUtil = mock(AgentUtil.class);
    when(agentUtil.findWarLocation("geode-web-management")).thenReturn(new URI("management.war"));

    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(managementService).isManager();
    verify(managementService, never()).startManager();
  }

  @Test
  public void startClusterManagementServiceWithRestServiceDisabledNeverInvokesStartManager()
      throws URISyntaxException {
    LoggingSession loggingSession = mock(LoggingSession.class);
    DistributionConfigImpl distributionConfig = mock(DistributionConfigImpl.class);
    when(distributionConfig.getJmxManager()).thenReturn(true);
    when(distributionConfig.getLocators()).thenReturn("");
    when(distributionConfig.getSecurableCommunicationChannels())
        .thenReturn(new SecurableCommunicationChannel[] {});
    when(distributionConfig.getSecurityAuthTokenEnabledComponents()).thenReturn(new String[] {});
    InternalLocator internalLocator = new InternalLocator(0, loggingSession, null, null, null, null,
        null, null, distributionConfig, null);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(regionFactory);
    when(cache.getOptionalService(HttpService.class))
        .thenReturn(Optional.of(mock(HttpService.class)));
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    BaseManagementService managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);
    when(distributionConfig.getEnableManagementRestService()).thenReturn(false);
    AgentUtil agentUtil = mock(AgentUtil.class);
    when(agentUtil.findWarLocation("geode-web-management")).thenReturn(new URI("management.war"));

    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(distributionConfig).getEnableManagementRestService();
    verify(managementService, never()).startManager();
  }

  @Test
  public void startClusterManagementServiceWithRestServiceEnabledThrowsWhatStartManagerThrows()
      throws URISyntaxException {
    LoggingSession loggingSession = mock(LoggingSession.class);
    DistributionConfigImpl distributionConfig = mock(DistributionConfigImpl.class);
    when(distributionConfig.getJmxManager()).thenReturn(true);
    when(distributionConfig.getLocators()).thenReturn("");
    when(distributionConfig.getSecurableCommunicationChannels())
        .thenReturn(new SecurableCommunicationChannel[] {});
    when(distributionConfig.getSecurityAuthTokenEnabledComponents()).thenReturn(new String[] {});
    InternalLocator internalLocator = new InternalLocator(0, loggingSession, null, null, null, null,
        null, null, distributionConfig, null);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(regionFactory);
    when(cache.getOptionalService(HttpService.class))
        .thenReturn(Optional.of(mock(HttpService.class)));
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    BaseManagementService managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    RuntimeException startManagerEx = new RuntimeException("startManager failed");
    doThrow(startManagerEx).when(managementService).startManager();
    AgentUtil agentUtil = mock(AgentUtil.class);
    when(agentUtil.findWarLocation("geode-web-management")).thenReturn(new URI("management.war"));

    Throwable throwable =
        catchThrowable(() -> internalLocator.startClusterManagementService(cache, agentUtil));

    assertThat(throwable).isSameAs(startManagerEx);
  }

}
