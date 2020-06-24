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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.tcpserver.ClusterSocketCreator;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorBuilder;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class CacheServerImplTest {

  private InternalCache cache;
  private CacheClientNotifier cacheClientNotifier;
  private ClientHealthMonitor clientHealthMonitor;
  private DistributionConfig config;
  private SecurityService securityService;
  private SocketCreator socketCreator;
  private InternalDistributedSystem system;
  private CacheServerAdvisor advisor;

  @Before
  public void setUp() throws IOException {
    cache = mock(InternalCache.class);
    cacheClientNotifier = mock(CacheClientNotifier.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);
    config = mock(DistributionConfig.class);
    securityService = mock(SecurityService.class);
    socketCreator = mock(SocketCreator.class);
    ClusterSocketCreator ssc = mock(ClusterSocketCreator.class);
    when(socketCreator.forCluster()).thenReturn(ssc);
    system = mock(InternalDistributedSystem.class);
    advisor = mock(CacheServerAdvisor.class);

    ServerSocket serverSocket = mock(ServerSocket.class);
    StatisticsManager statisticsManager = mock(StatisticsManager.class);

    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.getCacheTransactionManager()).thenReturn(mock(TXManagerImpl.class));
    when(serverSocket.getLocalSocketAddress()).thenReturn(mock(SocketAddress.class));
    when(socketCreator.createServerSocket(anyInt(), anyInt(), any(), any(), anyInt()))
        .thenReturn(serverSocket);
    when(ssc.createServerSocket(anyInt(), anyInt(), any()))
        .thenReturn(serverSocket);
    when(statisticsManager.createAtomicStatistics(any(), any())).thenReturn(mock(Statistics.class));
    when(statisticsManager.createType(any(), any(), any())).thenReturn(mock(StatisticsType.class));
    when(system.getConfig()).thenReturn(config);
    when(system.getProperties()).thenReturn(new Properties());
    when(system.getStatisticsManager()).thenReturn(statisticsManager);
  }

  @Test
  public void createdAcceptorIsGatewayEndpoint() throws IOException {
    OverflowAttributes overflowAttributes = mock(OverflowAttributes.class);
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));

    Acceptor acceptor = server.createAcceptor(overflowAttributes);

    assertThat(acceptor.isGatewayReceiver()).isFalse();
  }

  @Test
  public void getGroups_returnsSpecifiedGroup() {
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));
    String specifiedGroup = "group0";

    server.setGroups(new String[] {specifiedGroup});

    assertThat(server.getGroups())
        .containsExactly(specifiedGroup);
  }

  @Test
  public void getGroups_returnsMultipleSpecifiedGroups() {
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));
    String specifiedGroup1 = "group1";
    String specifiedGroup2 = "group2";
    String specifiedGroup3 = "group3";

    server.setGroups(new String[] {specifiedGroup1, specifiedGroup2, specifiedGroup3});

    assertThat(server.getGroups())
        .containsExactlyInAnyOrder(specifiedGroup1, specifiedGroup2, specifiedGroup3);
  }

  @Test
  public void getCombinedGroups_includesMembershipGroup() {
    String membershipGroup = "group-m0";
    when(config.getGroups()).thenReturn(membershipGroup);
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));

    assertThat(server.getCombinedGroups())
        .contains(membershipGroup);
  }

  @Test
  public void getCombinedGroups_includesMultipleMembershipGroups() {
    String membershipGroup1 = "group-m1";
    String membershipGroup2 = "group-m2";
    String membershipGroup3 = "group-m3";
    when(config.getGroups())
        .thenReturn(membershipGroup1 + "," + membershipGroup2 + "," + membershipGroup3);
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));

    assertThat(server.getCombinedGroups())
        .contains(membershipGroup1, membershipGroup2, membershipGroup3);
  }

  @Test
  public void getCombinedGroups_includesSpecifiedGroupsAndMembershipGroups() {
    String membershipGroup1 = "group-m1";
    String membershipGroup2 = "group-m2";
    String membershipGroup3 = "group-m3";
    when(config.getGroups())
        .thenReturn(membershipGroup1 + "," + membershipGroup2 + "," + membershipGroup3);
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));
    String specifiedGroup1 = "group1";
    String specifiedGroup2 = "group2";
    String specifiedGroup3 = "group3";

    server.setGroups(new String[] {specifiedGroup1, specifiedGroup2, specifiedGroup3});

    assertThat(server.getCombinedGroups())
        .containsExactlyInAnyOrder(membershipGroup1, membershipGroup2, membershipGroup3,
            specifiedGroup1, specifiedGroup2, specifiedGroup3);
  }

  @Test
  public void startNotifiesResourceEventCacheServerStart() throws IOException {
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));

    server.start();

    verify(system).handleResourceEvent(same(ResourceEvent.CACHE_SERVER_START), same(server));
  }

  @Test
  public void stopNotifiesResourceEventCacheServerStart() throws IOException {
    InternalCacheServer server = new CacheServerImpl(cache, securityService,
        StatisticsClockFactory.disabledClock(), new AcceptorBuilder(),
        true, true, () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, a -> advisor, new ServiceLoaderModuleService(
            LogService.getLogger()));
    server.start();

    server.stop();

    verify(system).handleResourceEvent(same(ResourceEvent.CACHE_SERVER_STOP), same(server));
  }
}
