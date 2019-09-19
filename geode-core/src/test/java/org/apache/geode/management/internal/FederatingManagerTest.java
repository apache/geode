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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
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
    InternalDistributedMember member = member(0, 42);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(cacheForClientAccess).createInternalRegion(eq("_monitoringRegion_null<v0>42"), any(),
        any());
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() throws Exception {
    InternalDistributedMember member = member(0, 42);
    FederatingManager federatingManager = new FederatingManager(jmxAdapter, repo, system, service,
        cache, statisticsFactory, statisticsClock);
    federatingManager.startManager();

    federatingManager.addMemberArtifacts(member);

    verify(cacheForClientAccess).createInternalRegion(eq("_notificationRegion_null<v0>42"), any(),
        any());
  }

  private InternalDistributedMember member(int viewId, int port) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(mock(InetAddress.class));
    when(member.getVmViewId()).thenReturn(viewId);
    when(member.getPort()).thenReturn(port);
    return member;
  }
}
