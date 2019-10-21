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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.DistributedSystemMXBean;

public class FederatingManagerTest {

  private MBeanJMXAdapter jmxAdapter;
  private ManagementResourceRepo repo;
  private InternalDistributedSystem system;
  private SystemManagementService service;
  private InternalCache cache;
  private StatisticsFactory statisticsFactory;
  private StatisticsClock statisticsClock;
  private InternalCacheForClientAccess cacheForClientAccess;

  @Before
  public void setUp() throws Exception {
    jmxAdapter = mock(MBeanJMXAdapter.class);
    repo = mock(ManagementResourceRepo.class);
    system = mock(InternalDistributedSystem.class);
    service = mock(SystemManagementService.class);
    cache = mock(InternalCache.class);
    statisticsFactory = mock(StatisticsFactory.class);
    statisticsClock = mock(StatisticsClock.class);
    cacheForClientAccess = mock(InternalCacheForClientAccess.class);
    DistributedSystemMXBean distributedSystemMXBean = mock(DistributedSystemMXBean.class);

    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
    when(cacheForClientAccess.createInternalRegion(any(), any(), any()))
        .thenReturn(mock(Region.class));
    when(distributedSystemMXBean.getAlertLevel())
        .thenReturn(AlertLevel.WARNING.name());
    when(jmxAdapter.getDistributedSystemMXBean())
        .thenReturn(distributedSystemMXBean);
    when(system.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegion() throws Exception {
    InternalDistributedMember member = member(1, 20);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(cacheForClientAccess)
        .createInternalRegion(eq("_monitoringRegion_null<v1>20"), any(), any());
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegionWithHasOwnStats() throws Exception {
    InternalDistributedMember member = member(2, 40);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    ArgumentCaptor<InternalRegionArguments> captor =
        ArgumentCaptor.forClass(InternalRegionArguments.class);
    verify(cacheForClientAccess)
        .createInternalRegion(eq("_monitoringRegion_null<v2>40"), any(), captor.capture());

    InternalRegionArguments internalRegionArguments = captor.getValue();
    HasCachePerfStats hasCachePerfStats = internalRegionArguments.getCachePerfStatsHolder();
    assertThat(hasCachePerfStats.hasOwnStats()).isTrue();
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() throws Exception {
    InternalDistributedMember member = member(3, 60);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(cacheForClientAccess)
        .createInternalRegion(eq("_notificationRegion_null<v3>60"), any(), any());
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegionWithHasOwnStats() throws Exception {
    InternalDistributedMember member = member(4, 80);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    ArgumentCaptor<InternalRegionArguments> captor =
        ArgumentCaptor.forClass(InternalRegionArguments.class);
    verify(cacheForClientAccess)
        .createInternalRegion(eq("_notificationRegion_null<v4>80"), any(), captor.capture());

    InternalRegionArguments internalRegionArguments = captor.getValue();
    HasCachePerfStats hasCachePerfStats = internalRegionArguments.getCachePerfStatsHolder();
    assertThat(hasCachePerfStats.hasOwnStats()).isTrue();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysMonitoringRegion() {
    InternalDistributedMember member = member(4, 80);
    Region monitoringRegion = mock(Region.class);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsLocallyDestroysNotificationRegion() {
    InternalDistributedMember member = member(4, 80);
    Region notificationRegion = mock(Region.class);
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfMonitoringRegionIsAlreadyDestroyed() {
    InternalDistributedMember member = member(4, 80);
    Region monitoringRegion = mock(Region.class);
    doThrow(new RegionDestroyedException("test", "monitoringRegion"))
        .when(monitoringRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(monitoringRegion);
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);

    federatingManager.removeMemberArtifacts(member, false);

    verify(monitoringRegion)
        .localDestroyRegion();
  }

  @Test
  public void removeMemberArtifactsDoesNotThrowIfNotificationRegionIsAlreadyDestroyed() {
    InternalDistributedMember member = member(4, 80);
    Region notificationRegion = mock(Region.class);
    doThrow(new RegionDestroyedException("test", "notificationRegion"))
        .when(notificationRegion)
        .localDestroyRegion();
    when(repo.getEntryFromMonitoringRegionMap(eq(member)))
        .thenReturn(mock(Region.class));
    when(repo.getEntryFromNotifRegionMap(eq(member)))
        .thenReturn(notificationRegion);
    when(system.getDistributedMember())
        .thenReturn(member);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);

    federatingManager.removeMemberArtifacts(member, false);

    verify(notificationRegion)
        .localDestroyRegion();
  }

  private InternalDistributedMember member(int viewId, int port) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(mock(InetAddress.class));
    when(member.getVmViewId()).thenReturn(viewId);
    when(member.getPort()).thenReturn(port);
    return member;
  }
}
