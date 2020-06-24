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

package org.apache.geode.distributed.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.management.internal.BaseManagementService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;

public class InternalLocatorTest {
  private InternalLocator internalLocator; // the instance under test
  private DistributionConfigImpl distributionConfig;
  private InternalCacheForClientAccess cache;
  private BaseManagementService managementService;
  private AgentUtil agentUtil;
  private HttpService httpService;
  private InternalDistributedSystem internalDistributedSystem;

  @Before
  public void setup() throws URISyntaxException {
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    distributionConfig = mock(DistributionConfigImpl.class);
    cache = mock(InternalCacheForClientAccess.class);
    managementService = mock(BaseManagementService.class);
    agentUtil = mock(AgentUtil.class);
    httpService = mock(HttpService.class);
    InternalRegionFactory regionFactory = mock(InternalRegionFactory.class);
    LoggingSession loggingSession = mock(LoggingSession.class);
    URI uri = new URI("file", "/management.war", null);

    when(distributionConfig.getJmxManager()).thenReturn(true);
    when(distributionConfig.getJmxManagerPort())
        .thenReturn(AvailablePortHelper.getRandomAvailableTCPPort());
    when(distributionConfig.getLocators()).thenReturn("");
    when(distributionConfig.getSecurableCommunicationChannels())
        .thenReturn(new SecurableCommunicationChannel[] {});
    when(distributionConfig.getSecurityAuthTokenEnabledComponents()).thenReturn(new String[] {});
    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(regionFactory);
    when(cache.getOptionalService(HttpService.class))
        .thenReturn(Optional.of(httpService));
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    // when(internalLocator.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(agentUtil.findWarLocation("geode-web-management")).thenReturn(uri);
    BaseManagementService.setManagementService(cache, managementService);

    internalLocator = new InternalLocator(0, loggingSession, null, null, null, null,
        null, null, distributionConfig, null,
        new ServiceLoaderModuleService(LogService.getLogger()));
  }

  @After
  public void tearDown() {
    BaseManagementService.clearManagementService(cache);
    if (internalLocator != null) {
      internalLocator.stop();
    }
  }

  @Test
  public void startClusterManagementServiceWithRestServiceEnabledInvokesStartManager()
      throws Exception {
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(managementService).startManager();
    verify(httpService).addWebApplication(eq("/management"), any(), any());
  }

  @Test
  public void startClusterManagementServiceWithRunningManagerNeverInvokesStartManager()
      throws Exception {
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    when(managementService.isManager()).thenReturn(true);

    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(managementService).isManager();
    verify(managementService, never()).startManager();
    verify(httpService).addWebApplication(eq("/management"), any(), any());
  }

  @Test
  public void startClusterManagementServiceWithRestServiceDisabledNeverInvokesStartManager()
      throws Exception {
    when(distributionConfig.getEnableManagementRestService()).thenReturn(false);
    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(distributionConfig).getEnableManagementRestService();
    verify(managementService, never()).startManager();
    verify(httpService, never()).addWebApplication(eq("/management"), any(), any());
  }

  @Test
  public void startClusterManagementServiceWithRestServiceEnabledDoesNotThrowWhenStartManagerThrows()
      throws Exception {
    when(distributionConfig.getEnableManagementRestService()).thenReturn(true);
    RuntimeException startManagerEx = new RuntimeException("startManager failed");
    doThrow(startManagerEx).when(managementService).startManager();

    internalLocator.startClusterManagementService(cache, agentUtil);

    verify(managementService).startManager();
    verify(httpService, never()).addWebApplication(eq("/management"), any(), any());
  }

}
