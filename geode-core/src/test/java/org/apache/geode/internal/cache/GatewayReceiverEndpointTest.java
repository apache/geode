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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.wan.GatewayReceiverEndpoint;
import org.apache.geode.internal.cache.wan.GatewayReceiverMetrics;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.test.junit.categories.WanTest;

@Category(WanTest.class)
public class GatewayReceiverEndpointTest {

  private InternalCache cache;
  private SecurityService securityService;
  private GatewayReceiver gatewayReceiver;
  private GatewayReceiverMetrics gatewayReceiverMetrics;
  private SocketCreator socketCreator;
  private CacheClientNotifier cacheClientNotifier;
  private ClientHealthMonitor clientHealthMonitor;
  private DistributionConfig config;

  @Before
  public void setUp() throws IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    ServerSocket serverSocket = mock(ServerSocket.class);
    StatisticsManager statisticsManager = mock(StatisticsManager.class);

    cache = mock(InternalCache.class);
    config = mock(DistributionConfig.class);
    securityService = mock(SecurityService.class);
    gatewayReceiver = mock(GatewayReceiver.class);
    gatewayReceiverMetrics = new GatewayReceiverMetrics(meterRegistry);
    socketCreator = mock(SocketCreator.class);
    cacheClientNotifier = mock(CacheClientNotifier.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);

    when(cache.getCCPTimer()).thenReturn(mock(SystemTimer.class));
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(serverSocket.getLocalSocketAddress()).thenReturn(mock(SocketAddress.class));
    when(socketCreator.createServerSocket(anyInt(), anyInt(), any(), any(), anyInt()))
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
    InternalCacheServer server = new GatewayReceiverEndpoint(cache, securityService,
        gatewayReceiver, gatewayReceiverMetrics, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    Acceptor acceptor = server.createAcceptor(overflowAttributes);

    assertThat(acceptor.isGatewayReceiver()).isTrue();
  }

  @Test
  public void cacheServer_getCombinedGroups_doesNotIncludeMembershipGroup() {
    String membershipGroup = "group-m0";
    when(config.getGroups()).thenReturn(membershipGroup);
    GatewayReceiverEndpoint server = new GatewayReceiverEndpoint(cache, securityService,
        gatewayReceiver, gatewayReceiverMetrics, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    assertThat(server.getCombinedGroups())
        .doesNotContain(membershipGroup);
  }
}
