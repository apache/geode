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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewayReceiverMetrics;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorImplTest {

  private MeterRegistry meterRegistry;
  private GatewayReceiverMetrics gatewayReceiverMetrics;
  private InternalCache cache;
  private ServerConnectionFactory serverConnectionFactory;
  private GatewayReceiver gatewayReceiver;
  private SocketCreator socketCreator;
  private SecurityService securityService;

  @Before
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    gatewayReceiverMetrics = new GatewayReceiverMetrics(meterRegistry);
    cache = mock(InternalCache.class);
    serverConnectionFactory = mock(ServerConnectionFactory.class);
    gatewayReceiver = mock(GatewayReceiver.class);
    socketCreator = mock(SocketCreator.class);
    securityService = mock(SecurityService.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    StatisticsManager statisticsManager = mock(StatisticsManager.class);

    when(cache.getCCPTimer()).thenReturn(mock(SystemTimer.class));
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(socketCreator.createServerSocket(anyInt(), anyInt(), isNull(),
        anyList(), anyInt()))
            .thenReturn(new SocketCreator(new SSLConfig()).createServerSocket(0, 0));
    when(statisticsManager.createAtomicStatistics(any(StatisticsType.class), anyString()))
        .thenReturn(mock(Statistics.class));
    when(statisticsManager.createType(anyString(), anyString(), notNull()))
        .thenReturn(mock(StatisticsType.class));
    when(system.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(system.getProperties()).thenReturn(new Properties());
    when(system.getStatisticsManager()).thenReturn(statisticsManager);
  }

  @After
  public void tearDown() {
    meterRegistry.close();
    if (CacheClientNotifier.getInstance() != null) {
      CacheClientNotifier.getInstance().shutdown(0);
    }
    ClientHealthMonitor.shutdownInstance();
  }

  @Test
  public void constructorWithoutGatewayReceiverCreatesAcceptorImplForCacheServer()
      throws Exception {
    AcceptorImpl acceptorImpl = createAcceptorImplForCacheServer();

    assertThat(acceptorImpl.isGatewayReceiver()).isFalse();
  }

  @Test
  public void constructorWithGatewayReceiverCreatesAcceptorImplForGatewayReceiver()
      throws Exception {
    AcceptorImpl acceptorImpl = createAcceptorImplForGatewayReceiver();

    assertThat(acceptorImpl.isGatewayReceiver()).isTrue();
  }

  private AcceptorImpl createAcceptorImplForCacheServer() throws IOException {
    return new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, cache, AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        0, CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,
        null, null, CacheServer.DEFAULT_TCP_NO_DELAY, serverConnectionFactory, 1000,
        securityService, null, null, Collections.emptyList(), () -> socketCreator);
  }

  private AcceptorImpl createAcceptorImplForGatewayReceiver() throws IOException {
    return new AcceptorImpl(0, null, false, CacheServer.DEFAULT_SOCKET_BUFFER_SIZE,
        CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, cache, AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        0, CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE,
        null, null, CacheServer.DEFAULT_TCP_NO_DELAY, serverConnectionFactory, 1000,
        securityService, gatewayReceiver, gatewayReceiverMetrics, Collections.emptyList(),
        () -> socketCreator);
  }
}
