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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.DistributedSystemMXBean;

public class LocalManagerTest {

  private ManagementResourceRepo repo;
  private InternalDistributedSystem system;
  private SystemManagementService service;
  private InternalCache cache;
  private InternalRegionFactory regionFactory1;
  private InternalRegionFactory regionFactory2;
  private StatisticsFactory statisticsFactory;
  private StatisticsClock statisticsClock;
  private InternalCacheForClientAccess cacheForClientAccess;

  @Before
  public void setUp() throws Exception {
    repo = mock(ManagementResourceRepo.class);
    system = mock(InternalDistributedSystem.class);
    service = mock(SystemManagementService.class);
    cache = mock(InternalCache.class);
    regionFactory1 = mock(InternalRegionFactory.class);
    regionFactory2 = mock(InternalRegionFactory.class);
    statisticsFactory = mock(StatisticsFactory.class);
    statisticsClock = mock(StatisticsClock.class);
    cacheForClientAccess = mock(InternalCacheForClientAccess.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    DistributionConfig config = mock(DistributionConfig.class);

    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(regionFactory1).thenReturn(regionFactory2);
    when(regionFactory1.create(any())).thenReturn(mock(Region.class));
    when(regionFactory2.create(any())).thenReturn(mock(Region.class));
    when(config.getJmxManagerUpdateRate())
        .thenReturn(Integer.MAX_VALUE);
    when(distributedSystemMXBean.getAlertLevel())
        .thenReturn(AlertLevel.WARNING.name());
    when(system.getConfig())
        .thenReturn(config);
    when(system.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
  }

  @Test
  public void startLocalManagementCreatesMonitoringRegion() throws Exception {
    InternalDistributedMember member = member(1, 20);
    when(system.getDistributedMember()).thenReturn(member);
    LocalManager localManager =
        new LocalManager(repo, system, service, cache, statisticsFactory, statisticsClock);

    localManager.startManager();

    verify(regionFactory1).create("_monitoringRegion_null<v1>20");
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegionWithHasOwnStats() throws Exception {
    InternalDistributedMember member = member(2, 40);
    when(system.getDistributedMember()).thenReturn(member);
    LocalManager localManager =
        new LocalManager(repo, system, service, cache, statisticsFactory, statisticsClock);

    localManager.startManager();

    ArgumentCaptor<HasCachePerfStats> captor =
        ArgumentCaptor.forClass(HasCachePerfStats.class);
    verify(regionFactory1).setCachePerfStatsHolder(captor.capture());
    assertThat(captor.getValue().hasOwnStats()).isTrue();
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() throws Exception {
    InternalDistributedMember member = member(3, 60);
    when(system.getDistributedMember()).thenReturn(member);
    LocalManager localManager =
        new LocalManager(repo, system, service, cache, statisticsFactory, statisticsClock);

    localManager.startManager();

    verify(regionFactory2).create("_notificationRegion_null<v3>60");
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegionWithHasOwnStats() throws Exception {
    InternalDistributedMember member = member(4, 80);
    when(system.getDistributedMember()).thenReturn(member);
    LocalManager localManager =
        new LocalManager(repo, system, service, cache, statisticsFactory, statisticsClock);

    localManager.startManager();

    ArgumentCaptor<HasCachePerfStats> captor =
        ArgumentCaptor.forClass(HasCachePerfStats.class);
    verify(regionFactory2).setCachePerfStatsHolder(captor.capture());
    assertThat(captor.getValue().hasOwnStats()).isTrue();
  }

  private InternalDistributedMember member(int viewId, int port) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(mock(InetAddress.class));
    when(member.getVmViewId()).thenReturn(viewId);
    when(member.getMembershipPort()).thenReturn(port);
    return member;
  }
}
