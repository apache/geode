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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.List;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorBuilderTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Test
  public void forServerSetsPortFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int port = 42;
    when(server.getPort()).thenReturn(port);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getPort()).isEqualTo(port);
  }

  @Test
  public void forServerSetsBindAddressFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    String bindAddress = "bind-address";
    when(server.getBindAddress()).thenReturn(bindAddress);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getBindAddress()).isEqualTo(bindAddress);
  }

  @Test
  public void forServerSetsNotifyBySubscriptionFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    boolean notifyBySubscription = true;
    when(server.getNotifyBySubscription()).thenReturn(notifyBySubscription);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.isNotifyBySubscription()).isEqualTo(notifyBySubscription);
  }

  @Test
  public void forServerSetsSocketBufferSizeFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int socketBufferSize = 84;
    when(server.getSocketBufferSize()).thenReturn(socketBufferSize);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSocketBufferSize()).isEqualTo(socketBufferSize);
  }

  @Test
  public void forServerSetsMaximumTimeBetweenPingsFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int maximumTimeBetweenPings = 84;
    when(server.getMaximumTimeBetweenPings()).thenReturn(maximumTimeBetweenPings);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaximumTimeBetweenPings()).isEqualTo(maximumTimeBetweenPings);
  }

  @Test
  public void forServerSetsCacheFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    InternalCache cache = mock(InternalCache.class);
    when(server.getCache()).thenReturn(cache);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getCache()).isEqualTo(cache);
  }

  @Test
  public void forServerSetsMaxConnectionsFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int maxConnections = 99;
    when(server.getMaxConnections()).thenReturn(maxConnections);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaxConnections()).isEqualTo(maxConnections);
  }

  @Test
  public void forServerSetsMaxThreadsFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int maxThreads = 50;
    when(server.getMaxThreads()).thenReturn(maxThreads);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaxThreads()).isEqualTo(maxThreads);
  }

  @Test
  public void forServerSetsMaximumMessageCountFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int maximumMessageCount = 500;
    when(server.getMaximumMessageCount()).thenReturn(maximumMessageCount);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaximumMessageCount()).isEqualTo(maximumMessageCount);
  }

  @Test
  public void forServerSetsMessageTimeToLiveFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    int messageTimeToLive = 400;
    when(server.getMessageTimeToLive()).thenReturn(messageTimeToLive);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMessageTimeToLive()).isEqualTo(messageTimeToLive);
  }

  @Test
  public void forServerSetsConnectionListenerFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    ConnectionListener connectionListener = mock(ConnectionListener.class);
    when(server.getConnectionListener()).thenReturn(connectionListener);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getConnectionListener()).isEqualTo(connectionListener);
  }

  @Test
  public void forServerSetsTcpNoDelayFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    boolean tcpNoDelay = true;
    when(server.getTcpNoDelay()).thenReturn(tcpNoDelay);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.isTcpNoDelay()).isEqualTo(tcpNoDelay);
  }

  @Test
  public void forServerSetsTimeLimitMillisFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    long timeLimitMillis = Long.MAX_VALUE - 1;
    when(server.getTimeLimitMillis()).thenReturn(timeLimitMillis);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getTimeLimitMillis()).isEqualTo(timeLimitMillis);
  }

  @Test
  public void forServerSetsSecurityServiceFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    SecurityService securityService = mock(SecurityService.class);
    when(server.getSecurityService()).thenReturn(securityService);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSecurityService()).isEqualTo(securityService);
  }

  @Test
  public void forServerSetsSocketCreatorSupplierFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    Supplier<SocketCreator> socketCreatorSupplier = () -> mock(SocketCreator.class);
    when(server.getSocketCreatorSupplier()).thenReturn(socketCreatorSupplier);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSocketCreatorSupplier()).isEqualTo(socketCreatorSupplier);
  }

  @Test
  public void forServerSetsCacheClientNotifierProviderFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    CacheClientNotifierProvider cacheClientNotifierProvider =
        mock(CacheClientNotifierProvider.class);
    when(server.getCacheClientNotifierProvider()).thenReturn(cacheClientNotifierProvider);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getCacheClientNotifierProvider()).isEqualTo(cacheClientNotifierProvider);
  }

  @Test
  public void forServerSetsClientHealthMonitorProviderFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    ClientHealthMonitorProvider clientHealthMonitorProvider =
        mock(ClientHealthMonitorProvider.class);
    when(server.getClientHealthMonitorProvider()).thenReturn(clientHealthMonitorProvider);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getClientHealthMonitorProvider()).isEqualTo(clientHealthMonitorProvider);
  }

  @Test
  public void forServerDoesNotUnsetIsGatewayReceiver() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    AcceptorBuilder builder = new AcceptorBuilder();
    boolean isGatewayReceiver = true;
    builder.setIsGatewayReceiver(isGatewayReceiver);

    builder.forServer(server);

    assertThat(builder.isGatewayReceiver()).isEqualTo(isGatewayReceiver);
  }

  @Test
  public void forServerDoesNotUnsetGatewayTransportFilters() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    AcceptorBuilder builder = new AcceptorBuilder();
    List<GatewayTransportFilter> gatewayTransportFilters =
        singletonList(mock(GatewayTransportFilter.class));
    builder.setGatewayTransportFilters(gatewayTransportFilters);

    builder.forServer(server);

    assertThat(builder.getGatewayTransportFilters()).isEqualTo(gatewayTransportFilters);
  }

  @Test
  public void forServerSetsStatisticsClockFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    StatisticsClock statisticsClock = mock(StatisticsClock.class);
    when(server.getStatisticsClock()).thenReturn(statisticsClock);
    AcceptorBuilder builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getStatisticsClock()).isEqualTo(statisticsClock);
  }

  @Test
  public void setCacheReplacesCacheFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    InternalCache cacheFromServer = mock(InternalCache.class, "fromServer");
    when(server.getCache()).thenReturn(cacheFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    InternalCache cacheFromSetter = mock(InternalCache.class, "fromSetter");

    builder.setCache(cacheFromSetter);

    assertThat(builder.getCache()).isEqualTo(cacheFromSetter);
  }

  @Test
  public void setConnectionListenerReplacesConnectionListenerFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    ConnectionListener connectionListenerFromServer = mock(ConnectionListener.class, "fromServer");
    when(server.getConnectionListener()).thenReturn(connectionListenerFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    ConnectionListener connectionListenerFromSetter = mock(ConnectionListener.class, "fromSetter");

    builder.setConnectionListener(connectionListenerFromSetter);

    assertThat(builder.getConnectionListener()).isEqualTo(connectionListenerFromSetter);
  }

  @Test
  public void setSecurityServiceReplacesSecurityServiceFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    SecurityService securityServiceFromServer = mock(SecurityService.class, "fromServer");
    when(server.getSecurityService()).thenReturn(securityServiceFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    SecurityService securityServiceFromSetter = mock(SecurityService.class, "fromSetter");

    builder.setSecurityService(securityServiceFromSetter);

    assertThat(builder.getSecurityService()).isEqualTo(securityServiceFromSetter);
  }

  @Test
  public void setSocketCreatorSupplierReplacesSocketCreatorSupplierFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    Supplier<SocketCreator> socketCreatorSupplierFromServer = () -> mock(SocketCreator.class);
    when(server.getSocketCreatorSupplier()).thenReturn(socketCreatorSupplierFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    Supplier<SocketCreator> socketCreatorSupplierFromSetter = () -> mock(SocketCreator.class);

    builder.setSocketCreatorSupplier(socketCreatorSupplierFromSetter);

    assertThat(builder.getSocketCreatorSupplier()).isEqualTo(socketCreatorSupplierFromSetter);
  }

  @Test
  public void setCacheClientNotifierProviderReplacesCacheClientNotifierProviderFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    CacheClientNotifierProvider cacheClientNotifierProviderFromServer =
        mock(CacheClientNotifierProvider.class, "fromServer");
    when(server.getCacheClientNotifierProvider()).thenReturn(cacheClientNotifierProviderFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    CacheClientNotifierProvider cacheClientNotifierProviderFromSetter =
        mock(CacheClientNotifierProvider.class, "fromSetter");

    builder.setCacheClientNotifierProvider(cacheClientNotifierProviderFromSetter);

    assertThat(builder.getCacheClientNotifierProvider())
        .isEqualTo(cacheClientNotifierProviderFromSetter);
  }

  @Test
  public void setClientHealthMonitorProviderReplacesClientHealthMonitorProviderFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    ClientHealthMonitorProvider clientHealthMonitorProviderFromServer =
        mock(ClientHealthMonitorProvider.class, "fromServer");
    when(server.getClientHealthMonitorProvider()).thenReturn(clientHealthMonitorProviderFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    ClientHealthMonitorProvider clientHealthMonitorProviderFromSetter =
        mock(ClientHealthMonitorProvider.class, "fromSetter");

    builder.setClientHealthMonitorProvider(clientHealthMonitorProviderFromSetter);

    assertThat(builder.getClientHealthMonitorProvider())
        .isEqualTo(clientHealthMonitorProviderFromSetter);
  }

  @Test
  public void setStatisticsClockReplacesStatisticsClockFromServer() {
    InternalCacheServer server = mock(InternalCacheServer.class);
    StatisticsClock statisticsClockFromServer = mock(StatisticsClock.class, "fromServer");
    when(server.getStatisticsClock()).thenReturn(statisticsClockFromServer);
    AcceptorBuilder builder = new AcceptorBuilder();
    builder.forServer(server);
    StatisticsClock statisticsClockFromSetter = mock(StatisticsClock.class, "fromSetter");

    builder.setStatisticsClock(statisticsClockFromSetter);

    assertThat(builder.getStatisticsClock()).isEqualTo(statisticsClockFromSetter);
  }
}
