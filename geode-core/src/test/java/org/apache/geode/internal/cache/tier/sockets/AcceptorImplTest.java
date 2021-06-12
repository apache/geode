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
package org.apache.geode.internal.cache.tier.sockets;

import static java.util.Collections.emptyList;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_SOCKET_BUFFER_SIZE;
import static org.apache.geode.cache.server.CacheServer.DEFAULT_TCP_NO_DELAY;
import static org.apache.geode.internal.cache.tier.sockets.AcceptorImpl.MINIMUM_MAX_CONNECTIONS;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Properties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorImplTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private InternalCache cache;
  private CacheClientNotifier cacheClientNotifier;
  private ClientHealthMonitor clientHealthMonitor;
  private SecurityService securityService;
  private SocketCreator socketCreator;
  private StatisticsManager statisticsManager;
  private InternalDistributedSystem system;

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    cacheClientNotifier = mock(CacheClientNotifier.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);
    socketCreator = mock(SocketCreator.class);
    securityService = mock(SecurityService.class);
    system = mock(InternalDistributedSystem.class);
    statisticsManager = mock(StatisticsManager.class);

    ServerSocket serverSocket = mock(ServerSocket.class);

    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(serverSocket.getLocalSocketAddress()).thenReturn(mock(SocketAddress.class));
    when(socketCreator.createServerSocket(anyInt(), anyInt(), isNull(), anyList(), anyInt()))
        .thenReturn(serverSocket);
    when(system.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(system.getProperties()).thenReturn(new Properties());
  }

  @Test
  public void constructorWithoutGatewayReceiverCreatesAcceptorImplForCacheServer()
      throws Exception {
    Acceptor acceptor = new AcceptorImpl(0, null, false, DEFAULT_SOCKET_BUFFER_SIZE,
        DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, cache, MINIMUM_MAX_CONNECTIONS, 0,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null,
        null, DEFAULT_TCP_NO_DELAY, 1000, securityService,
        () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, false, emptyList(), disabledClock());

    assertThat(acceptor.isGatewayReceiver()).isFalse();
  }

  @Test
  public void constructorWithGatewayReceiverCreatesAcceptorImplForGatewayReceiver()
      throws Exception {
    when(system.getStatisticsManager()).thenReturn(statisticsManager);
    when(statisticsManager.createType(any(), any(), any())).thenReturn(mock(StatisticsType.class));
    when(statisticsManager.createAtomicStatistics(any(), any())).thenReturn(mock(Statistics.class));
    when(cache.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());

    Acceptor acceptor = new AcceptorImpl(0, null, false, DEFAULT_SOCKET_BUFFER_SIZE,
        DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, cache, MINIMUM_MAX_CONNECTIONS, 0,
        CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, null,
        null, DEFAULT_TCP_NO_DELAY, 1000, securityService,
        () -> socketCreator, (a, b, c, d, e, f, g, h, i) -> cacheClientNotifier,
        (a, b, c) -> clientHealthMonitor, true, emptyList(), disabledClock());

    assertThat(acceptor.isGatewayReceiver()).isTrue();
  }
}
