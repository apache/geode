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

import static java.text.MessageFormat.format;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_JMX_MANAGER_UPDATE_RATE;
import static org.apache.geode.management.internal.ManagementConstants.MONITORING_REGION;
import static org.apache.geode.management.internal.ManagementConstants.NOTIFICATION_REGION;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.Notification;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class LocalManagerTest {

  private ArgumentCaptor<HasCachePerfStats> managementRegionStatsCaptor;

  private InternalCache cache;
  private ConcurrentHashMap<ObjectName, FederationComponent> federatedComponents;
  private HasCachePerfStats hasCachePerfStats;
  private Object lock;
  private InternalRegionFactory<String, Object> monitoringRegionFactory;
  private InternalRegionFactory<NotificationKey, Notification> notificationRegionFactory;
  private ManagementResourceRepo repo;
  private ScheduledExecutorService scheduledExecutorService;
  private SystemManagementService service;

  private LocalManager localManager;

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    managementRegionStatsCaptor = ArgumentCaptor.forClass(HasCachePerfStats.class);

    cache = mock(InternalCache.class);
    federatedComponents = new ConcurrentHashMap<>();
    hasCachePerfStats = mock(HasCachePerfStats.class);
    lock = new Object();
    monitoringRegionFactory = uncheckedCast(mock(InternalRegionFactory.class));
    notificationRegionFactory = uncheckedCast(mock(InternalRegionFactory.class));
    repo = mock(ManagementResourceRepo.class);
    scheduledExecutorService = mock(ScheduledExecutorService.class);
    service = mock(SystemManagementService.class);

    InternalCacheForClientAccess cacheForClientAccess = mock(InternalCacheForClientAccess.class);

    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
    when(cacheForClientAccess.createInternalRegionFactory())
        .thenReturn(uncheckedCast(monitoringRegionFactory))
        .thenReturn(uncheckedCast(notificationRegionFactory));
    when(monitoringRegionFactory.create(any()))
        .thenReturn(uncheckedCast(mock(Region.class, "monitoringRegion")));
    when(notificationRegionFactory.create(any()))
        .thenReturn(uncheckedCast(mock(Region.class, "notificationRegion")));
  }

  @Test
  public void startLocalManagementCreatesMonitoringRegion() {
    InternalDistributedMember member = member(1, 10);

    localManager =
        new LocalManager(repo, cache, member, service, lock, DEFAULT_JMX_MANAGER_UPDATE_RATE,
            federatedComponents, () -> hasCachePerfStats, () -> scheduledExecutorService);

    localManager.startManager();

    String regionName = managementRegionName(MONITORING_REGION, member);
    verify(monitoringRegionFactory).create(regionName);
  }

  @Test
  public void addMemberArtifactsCreatesMonitoringRegionWithHasOwnStats() {
    when(hasCachePerfStats.hasOwnStats())
        .thenReturn(true);
    InternalDistributedMember member = member(2, 20);
    localManager =
        new LocalManager(repo, cache, member, service, lock, DEFAULT_JMX_MANAGER_UPDATE_RATE,
            federatedComponents, () -> hasCachePerfStats, () -> scheduledExecutorService);

    localManager.startManager();

    verify(monitoringRegionFactory).setCachePerfStatsHolder(managementRegionStatsCaptor.capture());
    assertThat(managementRegionStatsCaptor.getValue().hasOwnStats()).isTrue();
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegion() {
    InternalDistributedMember member = member(3, 30);
    localManager =
        new LocalManager(repo, cache, member, service, lock, DEFAULT_JMX_MANAGER_UPDATE_RATE,
            federatedComponents, () -> hasCachePerfStats, () -> scheduledExecutorService);

    localManager.startManager();

    verify(notificationRegionFactory).create(managementRegionName(NOTIFICATION_REGION, member));
  }

  @Test
  public void addMemberArtifactsCreatesNotificationRegionWithHasOwnStats() {
    when(hasCachePerfStats.hasOwnStats())
        .thenReturn(true);
    InternalDistributedMember member = member(4, 40);
    localManager =
        new LocalManager(repo, cache, member, service, lock, DEFAULT_JMX_MANAGER_UPDATE_RATE,
            federatedComponents, () -> hasCachePerfStats, () -> scheduledExecutorService);

    localManager.startManager();

    verify(notificationRegionFactory)
        .setCachePerfStatsHolder(managementRegionStatsCaptor.capture());
    assertThat(managementRegionStatsCaptor.getValue().hasOwnStats()).isTrue();
  }

  private static InternalDistributedMember member(int viewId, int port) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    InetAddress inetAddress = mock(InetAddress.class);

    when(inetAddress.getHostAddress())
        .thenReturn("hostAddress");
    when(member.getInetAddress())
        .thenReturn(inetAddress);
    when(member.getVmViewId())
        .thenReturn(viewId);
    when(member.getMembershipPort())
        .thenReturn(port);

    return member;
  }

  private static String managementRegionName(String rootName, InternalDistributedMember member) {
    return managementRegionName(rootName, member.getInetAddress().getHostAddress(),
        member.getVmViewId(), member.getMembershipPort());
  }

  private static String managementRegionName(String rootName, String hostAddress, int viewId,
      int membershipPort) {
    return rootName + "_"
        + format("{0}<v{1}>{2}", hostAddress, viewId, membershipPort).toLowerCase();
  }
}
