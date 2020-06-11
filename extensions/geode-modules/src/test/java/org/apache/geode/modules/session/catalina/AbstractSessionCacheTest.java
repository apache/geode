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

package org.apache.geode.modules.session.catalina;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.servlet.http.HttpSession;

import org.apache.juli.logging.Log;
import org.junit.Test;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.modules.util.RegionConfiguration;

public abstract class AbstractSessionCacheTest {

  protected String sessionRegionName = "sessionRegion";
  private final String sessionRegionAttributesId = RegionShortcut.PARTITION.toString();
  private final boolean gatewayDeltaReplicationEnabled = true;
  private final boolean gatewayReplicationEnabled = true;
  private final boolean enableDebugListener = true;


  protected SessionManager sessionManager = mock(SessionManager.class);
  @SuppressWarnings("unchecked")
  protected Region<String, HttpSession> sessionRegion = mock(Region.class, RETURNS_DEEP_STUBS);
  protected DistributedSystem distributedSystem = mock(DistributedSystem.class);
  protected Log logger = mock(Log.class);
  protected Execution<Object, Object, List<Object>> emptyExecution =
      uncheckedCast(mock(Execution.class));

  protected AbstractSessionCache sessionCache;

  @Test
  public void createRegionConfigurationSetsAppropriateValuesWithDefaultMaxInactiveInterval() {
    final RegionConfiguration config = spy(new RegionConfiguration());
    doReturn(config).when(sessionCache).getNewRegionConfiguration();

    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(sessionRegionAttributesId);
    when(sessionManager.getMaxInactiveInterval())
        .thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL);
    when(sessionManager.getEnableGatewayDeltaReplication())
        .thenReturn(gatewayDeltaReplicationEnabled);
    when(sessionManager.getEnableGatewayReplication()).thenReturn(gatewayReplicationEnabled);
    when(sessionManager.getEnableDebugListener()).thenReturn(enableDebugListener);

    sessionCache.createRegionConfiguration();

    verify(config).setRegionName(sessionRegionName);
    verify(config).setRegionAttributesId(sessionRegionAttributesId);
    verify(config, times(0)).setMaxInactiveInterval(anyInt());
    verify(config, times(0)).setCustomExpiry(any(CustomExpiry.class));
    verify(config).setEnableGatewayDeltaReplication(gatewayDeltaReplicationEnabled);
    verify(config).setEnableGatewayReplication(gatewayReplicationEnabled);
    verify(config).setEnableDebugListener(enableDebugListener);
  }

  @Test
  public void createRegionConfigurationSetsAppropriateValuesWithNonDefaultMaxInactiveInterval() {
    final RegionConfiguration config = spy(new RegionConfiguration());
    doReturn(config).when(sessionCache).getNewRegionConfiguration();

    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(sessionRegionAttributesId);
    final int nonDefaultMaxInactiveInterval = RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL + 1;
    when(sessionManager.getMaxInactiveInterval()).thenReturn(nonDefaultMaxInactiveInterval);
    when(sessionManager.getEnableGatewayDeltaReplication())
        .thenReturn(gatewayDeltaReplicationEnabled);
    when(sessionManager.getEnableGatewayReplication()).thenReturn(gatewayReplicationEnabled);
    when(sessionManager.getEnableDebugListener()).thenReturn(enableDebugListener);

    sessionCache.createRegionConfiguration();

    verify(config).setRegionName(sessionRegionName);
    verify(config).setRegionAttributesId(sessionRegionAttributesId);
    verify(config).setMaxInactiveInterval(nonDefaultMaxInactiveInterval);
    verify(config).setCustomExpiry(any(CustomExpiry.class));
    verify(config).setEnableGatewayDeltaReplication(gatewayDeltaReplicationEnabled);
    verify(config).setEnableGatewayReplication(gatewayReplicationEnabled);
    verify(config).setEnableDebugListener(enableDebugListener);
  }

  @Test
  public void destroySessionDoesNotThrowExceptionWhenGetOperatingRegionThrowsEntryNotFoundException() {
    final EntryNotFoundException exception = new EntryNotFoundException("Entry not found.");
    final String sessionId = "sessionId";
    // For Client/Server the operating Region is always the session Region, for peer to peer this is
    // only true when
    // local caching is not enabled. For the purposes of this test the behavior is equivalent
    // regardless of local
    // caching.
    when(sessionCache.getOperatingRegion()).thenReturn(sessionRegion);
    doThrow(exception).when(sessionRegion).destroy(sessionId);

    sessionCache.destroySession(sessionId);
    verify(sessionCache).getOperatingRegion();
  }
}
